%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Jan 2019 20:13
%%%-------------------------------------------------------------------
-module(maxwell_frontend_route_mgr_mgr).
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  fetch_route_mgr/1,
  get_types/0,
  add_listener/1,
  delete_listener/1
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
-define(ON_UP_CMD(Pid), {'$on_up', Pid}).
-define(ON_DOWN_CMD(Pid), {'$on_down', Pid}).

-record(state, {
  type_pids, pid_types, listeners
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

fetch_route_mgr(Type) ->
  gen_server:call(?SERVER, {fetch_route_mgr, Type}).

get_types() ->
  gen_server:call(?SERVER, get_types).

add_listener(ListenerPid) ->
  gen_server:call(?SERVER, {add_listener, ListenerPid}).

delete_listener(ListenerPid) ->
  gen_server:call(?SERVER, {delete_listener, ListenerPid}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  State = init_state(),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  process_flag(trap_exit, true),
  {ok, State}.

handle_call({fetch_route_mgr, Type}, _From, State) ->
  reply(fetch_route_mgr2(Type, State));
handle_call({add_listener, ListenerPid}, _From, State) ->
  reply(add_listener2(ListenerPid, State));
handle_call({delete_listener, ListenerPid}, _From, State) ->
  reply(delete_listener2(ListenerPid, State));
handle_call(get_types, _From, State) ->
  reply(get_types2(State));
handle_call(Request, _From, State) ->
  lager:error("Received unknown call: ~p", [Request]),
  reply({ok, State}).

handle_cast(Request, State) ->
  lager:error("Received unknown cast: ~p", [Request]),
  noreply(State).

handle_info({'EXIT', FromPid, Reason}, State) ->
  lager:info("Route mgr was down: reason: ~p", [Reason]),
  noreply(on_route_mgr_down(FromPid, State));
handle_info(Info, State) ->
  lager:error("Received unknown Info: ~p", [Info]),
  noreply(State).

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
  #state{
    type_pids = dict:new(),
    pid_types = dict:new(),
    listeners = []
  }.

fetch_route_mgr2(Type, State) ->
  case dict:find(Type, State#state.type_pids) of
    error ->
      case maxwell_frontend_route_mgr:start_link(Type) of
        {ok, Pid} = Result ->
          State2 = notify_and_clear(?ON_UP_CMD(Pid), store(Type, Pid, State)),
          {Result, State2};
        {error, {already_started, Pid}} ->
          State2 = notify_and_clear(?ON_UP_CMD(Pid), store(Type, Pid, State)),
          {{ok, Pid}, State2}
      end;
    {ok, _} = Result -> {Result, State}
  end.

get_types2(State) ->
  {dict:fetch_keys(State#state.type_pids), State}.

add_listener2(ListenerPid, State) ->
  {ok, add_listener3(ListenerPid, State)}.

add_listener3(ListenerPid, State) ->
  case lists:member(ListenerPid, State#state.listeners) of
    true -> State;
    false ->
      State#state{
        listeners = lists:append(State#state.listeners, [ListenerPid])
      }
  end.

delete_listener2(ListenerPid, State) ->
  {ok, delete_listener3(ListenerPid, State)}.

delete_listener3(ListenerPid, State) ->
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

on_route_mgr_down(Pid, State) ->
  notify_and_clear(?ON_DOWN_CMD(Pid), erase_by_pid(Pid, State)).

store(Type, Pid, State) ->
  State#state{
    type_pids = dict:store(Type, Pid, State#state.type_pids),
    pid_types = dict:store(Pid, Type, State#state.pid_types)
  }.

erase(Type, Pid, State) ->
  State#state{
    type_pids = dict:erase(Type, State#state.type_pids),
    pid_types = dict:erase(Pid, State#state.pid_types)
  }.

erase_by_pid(Pid, State) ->
  case dict:find(Pid, State#state.pid_types) of
    {ok, Type} -> erase(Type, Pid, State);
    error -> State
  end.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.