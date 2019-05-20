%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Sep 2018 5:09 PM
%%%-------------------------------------------------------------------
-module(maxwell_frontend_handler_id_mgr).
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  assign_id/1,
  get_pid/1
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

-record(state, {id, id_refs, ref_ids}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

assign_id(Pid) ->
  gen_server:call(?SERVER, {assign_id, Pid}).

get_pid(Id) ->
  gen_server:call(?SERVER, {get_pid, Id}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
  {ok, init_state()}.

handle_call({assign_id, Pid}, _From, State) ->
  reply(assign_id0(Pid, State));
handle_call({get_pid, Id}, _From, State) ->
  reply(get_pid0(Id, State));
handle_call(_Request, _From, State) ->
  reply({ok, State}).

handle_cast(_Request, State) ->
  noreply(State).

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
  #state{
    id = -1,
    id_refs = dict:new(),
    ref_ids = dict:new()
  }.

assign_id0(Pid, State) ->
  NewState = assign_id1(Pid, State),
  {NewState#state.id, NewState}.

assign_id1(Pid, State) ->
  Id = State#state.id + 1,
  Ref = erlang:monitor(process, Pid),
  State#state{
    id = Id,
    id_refs = dict:store(Id, {Ref, Pid}, State#state.id_refs),
    ref_ids = dict:store(Ref, Id, State#state.ref_ids)
  }.

get_pid0(Id, State) ->
  {get_pid1(Id, State), State}.

get_pid1(Id, State) ->
  case dict:find(Id, State#state.id_refs) of
    {ok, {_Ref, Pid}} ->
      Pid;
    error ->
      undefined
  end.

on_process_down(Ref, State) ->
  case dict:find(Ref, State#state.ref_ids) of
    {ok, Id} ->
      erlang:demonitor(Ref),
      State#state{
        id_refs = dict:erase(Id, State#state.id_refs),
        ref_ids = dict:erase(Ref, State#state.ref_ids)
      };
    error ->
      State
  end.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.