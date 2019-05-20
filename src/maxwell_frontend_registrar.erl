%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2019 17:03
%%%-------------------------------------------------------------------
-module(maxwell_frontend_registrar).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

%% API
-export([start_link/0]).

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
-define(ON_CONNECTED_CMD(Ref, Pid), {'$on_connected', Ref, Pid}).
-define(ON_DISCONNECTED_CMD(Ref, Pid), {'$on_disconnected', Ref, Pid}).
-define(REGISTER_CMD, '$register').

-record(state, {
  registered
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  State = #state{registered = false},
  maxwell_frontend_master_connector:add_listener(self()),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(?ON_CONNECTED_CMD(_, Pid), State) ->
  lager:debug("ON_CONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_connected(State)};
handle_info(?ON_DISCONNECTED_CMD(_, Pid), State) ->
  lager:debug("ON_DISCONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_disconnected(State)};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(Reason, State) ->
  lager:info(
    "Terminating ~p: reason: ~p, state: ~p", [?MODULE, Reason, State]
  ),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

on_connected(State) ->
  register(State).

on_disconnected(State) ->
  State#state{registered = false}.

register(State) ->
  case State#state.registered of
    true -> State;
    false -> register2(State)
  end.

register2(State) ->
  Req = #register_frontend_req_t{
    endpoint = maxwell_server_config:get_public_endpoint()
  },
  lager:info("Registering frontend: ~p", [Req]),
  Rep = maxwell_frontend_master_connector:send(Req, 5000),
  register3(Rep, State).

register3(#register_frontend_rep_t{}, State) ->
  lager:info("Frontend was successfully registered!", []),
  State#state{registered = true};
register3(#error_rep_t{code = _, desc = _} = Rep, State) ->
  lager:info("Failed to register frontend: ~p", [Rep]),
  send_cmd(?REGISTER_CMD, 1000),
  State;
register3({error, {_, _}} = Error, State) ->
  lager:info("Failed to register frontend: ~p", [Error]),
  send_cmd(?REGISTER_CMD, 1000),
  State.

send_cmd(Cmd, DelayMS) ->
  erlang:send_after(DelayMS, self(), Cmd).