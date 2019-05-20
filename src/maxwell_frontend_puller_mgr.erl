%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Dec 2018 14:08
%%%-------------------------------------------------------------------
-module(maxwell_frontend_puller_mgr).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

%% API
-export([
  start_link/0,
  ensure_puller_started/1
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).
-define(CLEAN_CMD, '$clean').
-define(CLEAN_INTERVAL, 10000).
-define(MAX_IDLE_TIME, 60000).

-record(state, {topic_times, ref_topics}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

ensure_puller_started(Topic) ->
  gen_server:cast(?SERVER, {ensure_puller_started, Topic}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  send_cmd(?CLEAN_CMD, ?CLEAN_INTERVAL),
  {ok, init_state()}.

handle_call(_Request, _From, State) ->
  reply({ok, State}).

handle_cast({ensure_puller_started, Topic}, State) ->
  noreply(ensure_puller_started0(Topic, State));
handle_cast(_Request, State) ->
  noreply(State).

handle_info(?CLEAN_CMD, State) ->
  noreply(clean_expired_pullers(State));
handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
  lager:debug(
    "Monitored process went down: ref: ~p, pid: ~p, reason: ~p",
    [Ref, Pid, Reason]
  ),
  noreply(on_process_down(Ref, State));
handle_info(_Info, State) ->
  noreply(State).

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_state() ->
  #state{topic_times = dict:new(), ref_topics = dict:new()}.

ensure_puller_started0(Topic, State) ->
  ensure_puller_started1(Topic, State).

ensure_puller_started1(Topic, State) ->
  case dict:find(Topic, State#state.topic_times) of
    {ok, _} -> update_puller(Topic, State);
    error -> start_puller(Topic, State)
  end.

start_puller(Topic, State) ->
  case resolve_backend(Topic, 5000) of
    {ok, Endpoint} ->
      {ok, Pid} = maxwell_client_puller:ensure_started(Topic, Endpoint),
      Ref = erlang:monitor(process, Pid),
      State#state{
        topic_times = dict:store(Topic, current(), State#state.topic_times),
        ref_topics = dict:store(Ref, Topic, State#state.ref_topics)
      };
    Error ->
      lager:error(
        "Failed to resolve backend: topic: ~p, reason: ~p", [Topic, Error]
      ),
      State
  end.

resolve_backend(Topic, Timeout) ->
  Req = #resolve_backend_req_t{topic = Topic},
  Rep = maxwell_frontend_master_connector:send(Req, Timeout),
  resolve_backend2(Rep).

resolve_backend2(#resolve_backend_rep_t{endpoint = Endpoint}) ->
  {ok, Endpoint};
resolve_backend2(#error_rep_t{code = Code, desc = Desc}) ->
  {error, {Code, Desc}};
resolve_backend2({error, {_, _}} = Error) ->
  Error.

update_puller(Topic, State) ->
  State#state{
    topic_times = dict:store(Topic, current(), State#state.topic_times)
  }.

clean_expired_pullers(State) ->
  State2 = dict:fold(
    fun(Ref, Topic, State0) ->
      case dict:find(Topic, State0#state.topic_times) of
        {ok, Time} ->
          case current() - Time >= ?MAX_IDLE_TIME of
            true -> stop_puller(Topic, Ref, State0);
            false -> State0
          end;
        error -> stop_puller(Topic, Ref, State0)
      end
    end,
    State,
    State#state.ref_topics
  ),
  send_cmd(?CLEAN_CMD, ?CLEAN_INTERVAL),
  State2.

stop_puller(Topic, Ref, State) ->
  maxwell_client_puller:stop(Topic),
  State#state{
    topic_times = dict:erase(Topic, State#state.topic_times),
    ref_topics = dict:erase(Ref, State#state.ref_topics)
  }.

on_process_down(Ref, State) ->
  case dict:find(Ref, State#state.ref_topics) of
    {ok, Topic} ->
      erlang:demonitor(Ref),
      State#state{
        topic_times = dict:erase(Topic, State#state.topic_times),
        ref_topics = dict:erase(Ref, State#state.ref_topics)
      };
    error -> State
  end.

send_cmd(Cmd, DelayMs) ->
  erlang:send_after(DelayMs, self(), Cmd).

current() ->
  {MegaSec, Sec, _} = os:timestamp(),
  1000000 * MegaSec + Sec.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.