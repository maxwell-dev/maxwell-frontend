%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Jul 2018 4:21 PM
%%%-------------------------------------------------------------------
-module(maxwell_frontend_master_connector).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

%% API
-export([
  start_link/0,
  add_listener/1,
  delete_listener/1,
  send/2
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
-define(ON_CONNECTED_CMD(Ref, Pid), {'$on_connected', Ref, Pid}).
-define(ON_DISCONNECTED_CMD(Ref, Pid), {'$on_disconnected', Ref, Pid}).

-record(state, {
  endpoints,
  endpoint_index,
  conn_ref,
  conn_pid,
  listeners
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

add_listener(ListenerPid) ->
  gen_server:call(?SERVER, {add_listener, ListenerPid}).

delete_listener(ListenerPid) ->
  gen_server:call(?SERVER, {delete_listener, ListenerPid}).

send(Msg, Timeout) ->
  gen_server:call(?SERVER, {send, Msg, Timeout}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
  State = fetch_conn(init_state()),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call({add_listener, ListenerPid}, _From, State) ->
  {reply, ok, add_listener2(ListenerPid, State)};
handle_call({delete_listener, ListenerPid}, _From, State) ->
  {reply, ok, delete_listener2(ListenerPid, State)};
handle_call({send, Msg, Timeout}, _From, State) ->
  {reply, send2(Msg, Timeout, State), State};
handle_call(Request, _From, State) ->
  lager:error("Recevied unknown call: ~p", [Request]),
  {reply, ok, State}.

handle_cast(Request, State) ->
  lager:error("Recevied unknown cast: ~p", [Request]),
  {noreply, State}.

handle_info(?ON_CONNECTED_CMD(_, Pid), State) ->
  lager:debug("ON_CONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_connected(State)};
handle_info(?ON_DISCONNECTED_CMD(_, Pid), State) ->
  lager:debug("ON_DISCONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_disconnected(State)};
handle_info({'DOWN', _ConnRef, process, _ConnPid, _Reason}, State) ->
  {noreply, fetch_conn(on_disconnected(State))};
handle_info(Info, State) ->
  lager:error("Recevied unknown info: ~p", [Info]),
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
init_state() ->
  Endpoints = maxwell_frontend_config:get_master_endpoints(),
  #state{endpoints = Endpoints, endpoint_index = 0, listeners = []}.

fetch_conn(State) ->
  {Endpoint, NewState} = next_endpoint(State),
  {ok, Pid} = maxwell_client_conn_sup:start_child(Endpoint),
  Ref = erlang:monitor(process, Pid),
  maxwell_client_conn:add_listener(Pid, self()),
  NewState#state{conn_ref = Ref, conn_pid = Pid}.

next_endpoint(State) ->
  NextIndex0 = State#state.endpoint_index + 1,
  NextIndex1 =
    case NextIndex0 =< length(State#state.endpoints) of
      true -> NextIndex0;
      false -> 1
    end,
  {
    lists:nth(NextIndex1, State#state.endpoints),
    State#state{endpoint_index = NextIndex1}
  }.

add_listener2(ListenerPid, State) ->
  case lists:member(ListenerPid, State#state.listeners) of
    true -> State;
    false ->
      State#state{
        listeners = lists:append(State#state.listeners, [ListenerPid])
      }
  end.

delete_listener2(ListenerPid, State) ->
  State#state{
    listeners = lists:delete(ListenerPid, State#state.listeners)
  }.

notify_and_clear(Msg, State) ->
  NewListeners = lists:filter(
    fun(ListenerPid) -> erlang:is_process_alive(ListenerPid) end,
    State#state.listeners
  ),
  lists:foreach(
    fun(ListenerPid) -> ListenerPid ! Msg end, NewListeners
  ),
  State#state{listeners = NewListeners}.

on_connected(State) ->
  notify_and_clear(
    ?ON_CONNECTED_CMD(State#state.conn_ref, State#state.conn_pid), State
  ).

on_disconnected(State) ->
  notify_and_clear(
    ?ON_DISCONNECTED_CMD(State#state.conn_ref, State#state.conn_pid), State
  ).

send2(Msg, Timeout, State) ->
  catch maxwell_client_conn:send(State#state.conn_pid, Msg, Timeout).