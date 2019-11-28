%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Sep 2018 3:54 PM
%%%-------------------------------------------------------------------
-module(maxwell_frontend_watcher_mgr).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

%% API
-export([
  start_link/1,
  add/2,
  remove/2,
  next/1,
  stop/1
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
-define(SHOULD_CLEAN_CMD, '$should_clean').
-define(PROCESS_NAME(Name), {watcher_mgr, Name}).
-define(VIA_PROCESS_NAME(Name),
  {via, maxwell_server_registry, ?PROCESS_NAME(Name)}
).

-record(state, {
  type,
  watchers,
  current
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Type) ->
  gen_server:start_link(?VIA_PROCESS_NAME(Type), ?MODULE, [Type], []).

add(Type, WatcherPid) ->
  {ok, MgrPid} = maxwell_frontend_watcher_mgr_mgr:fetch_watcher_mgr(Type),
  gen_server:call(MgrPid, {add, WatcherPid}).

remove(Type, WatcherPid) ->
  case (catch gen_server:call(?VIA_PROCESS_NAME(Type), {remove, WatcherPid})) of
    {'EXIT', {noproc, _}} -> ok;
    Result -> Result
  end.

next(Type) ->
  case (catch gen_server:call(?VIA_PROCESS_NAME(Type), next)) of
    {'EXIT', {noproc, _}} -> undefined;
    WatcherPid -> WatcherPid
  end.

stop(Type) -> gen_server:stop(?VIA_PROCESS_NAME(Type)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Type]) ->
  State = init_state(Type),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call({add, Pid}, _From, State) ->
  lager:debug("Adding watcher: type: ~p, pid: ~p", [State#state.type, Pid]),
  reply(add2(Pid, State));
handle_call({remove, Pid}, _From, State) ->
  lager:debug("Removing watcher: type: ~p, pid ~p", [State#state.type, Pid]),
  reply(remove2(Pid, State));
handle_call(next, _From, State) ->
  reply(next2(State));
handle_call(Request, _From, State) ->
  lager:error("Recevied unknown call: ~p", [Request]),
  reply({ok, State}).

handle_cast(Request, State) ->
  lager:error("Recevied unknown cast: ~p", [Request]),
  noreply(State).

handle_info(?SHOULD_CLEAN_CMD, State) ->
  {noreply, should_clean(State)};
handle_info({'DOWN', WatcherRef, process, _, Reason}, State) ->
  lager:info(
    "Watcher was down: name: ~p, watcher_ref: ~p, reason: ~p",
    [State#state.type, WatcherRef, Reason]
  ),
  noreply(on_watcher_down(WatcherRef, State));
handle_info(Info, State) ->
  lager:error("Recevied unknown info: ~p", [Info]),
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
init_state(Type) ->
  #state{
    type = Type,
    watchers = gb_trees:empty(),
    current = undefined
  }.

add2(WatcherPid, State) ->
  case gb_trees:is_defined(WatcherPid, State#state.watchers) of
    true ->
      lager:warning("Already existed: pid: ~p", [WatcherPid]),
      {ok, State};
    false ->
      WatcherRef = erlang:monitor(process, WatcherPid),
      {ok, State#state{
        watchers = gb_trees:insert(
          WatcherPid, WatcherRef, State#state.watchers
        )
      }}
  end.

remove2(WatcherPid, State) ->
  send_cmd(?SHOULD_CLEAN_CMD),
  {ok, State#state{watchers = remove_by_pid(WatcherPid, State)}}.

remove_by_pid(WatcherPid, Watchers) ->
  case gb_trees:lookup(WatcherPid, Watchers) of
    {value, WatcherRef} ->
      erlang:demonitor(WatcherRef),
      gb_trees:delete(WatcherPid, Watchers);
    none -> Watchers
  end.

next2(State) ->
  State2 = next3(State),
  case State2#state.current of
    undefined -> {undefined, State2};
    WatcherPid -> {WatcherPid, State2}
  end.

next3(State) ->
  case gb_trees:next(iter(State)) of
    {WatcherPid, _, _} -> State#state{current = WatcherPid};
    none -> State#state{current = undefined}
  end.

iter(State) -> %% return an iterator from current's next key
  Current = State#state.current,
  case Current =/= undefined of
    true ->
      Iter = gb_trees:iterator_from(Current, State#state.watchers),
      case gb_trees:next(Iter) of
        %% current is still there
        {Current, _, Iter2} ->
          {Largest, _} = gb_trees:largest(State#state.watchers),
          case Current =:= Largest of
            true -> gb_trees:iterator(State#state.watchers);
            false -> Iter2
          end;
        %% current has been deleted
        {_, _, _} -> Iter;
        %% current is last element and has been deleted,
        %% so rewind the iterator
        none -> gb_trees:iterator(State#state.watchers)
      end;
    false -> gb_trees:iterator(State#state.watchers)
  end.

should_clean(State) ->
  case gb_trees:size(State#state.watchers) =< 0 of
    true -> erlang:exit({shutdown, cleaned});
    false -> ignore
  end,
  State.

on_watcher_down(WatcherRef, State) ->
  send_cmd(?SHOULD_CLEAN_CMD),
  State#state{
    watchers = remove_by_ref(WatcherRef, State#state.watchers)
  }.

remove_by_ref(WatcherRef, Watchers) ->
  Iter = gb_trees:iterator(Watchers),
  remove_by_ref(gb_trees:next(Iter), WatcherRef, Watchers).

remove_by_ref({WatcherPid, WatcherRef2, Iter}, WatcherRef, Watchers) ->
  case WatcherRef =:= WatcherRef2 of
    true -> gb_trees:delete(WatcherPid, Watchers);
    false -> remove_by_ref(gb_trees:next(Iter), WatcherRef, Watchers)
  end;
remove_by_ref(none, _, Watchers) -> Watchers.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.

send_cmd(Cmd) ->
  erlang:send(self(), Cmd).