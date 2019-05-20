%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Sep 2018 3:54 PM
%%%-------------------------------------------------------------------
-module(maxwell_frontend_route_mgr).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

%% API
-export([
  start_link/1,
  ensure_started/1,
  add/2,
  remove/2,
  replace/2,
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
-define(PROCESS_NAME(Name), {route_mgr, Name}).
-define(VIA_PROCESS_NAME(Name),
  {via, maxwell_server_registry, ?PROCESS_NAME(Name)}
).

-record(state, {
  type,
  endpoints,
  current
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Type) ->
  gen_server:start_link(
    ?VIA_PROCESS_NAME(Type), ?MODULE, [Type], []
  ).

ensure_started(Type) ->
  case maxwell_server_registry:whereis_name(
    ?PROCESS_NAME(Type)) of
    undefined ->
      case maxwell_frontend_route_mgr_sup:start_child(Type) of
        {error, {already_started, Pid}} -> {ok, Pid};
        {ok, _} = Result -> Result
      end;
    Pid -> {ok, Pid}
  end.

add(Type, Endpoint) ->
  {ok, Pid} = ensure_started(Type),
  gen_server:call(Pid, {add, Endpoint}).

remove(Type, Endpoint) ->
  case (catch gen_server:call(?VIA_PROCESS_NAME(Type), {remove, Endpoint})) of
    {'EXIT', {noproc, _}} -> ok;
    Result -> Result
  end.

replace(Type, Endpoints) ->
  {ok, Pid} = ensure_started(Type),
  gen_server:call(Pid, {replace, Endpoints}).

next(Type) ->
  case (catch gen_server:call(?VIA_PROCESS_NAME(Type), next)) of
    {'EXIT', {noproc, _}} -> undefined;
    Endpoint -> Endpoint
  end.

stop(Type) ->
  gen_server:cast(?VIA_PROCESS_NAME(Type), stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Type]) ->
  State = init_state(Type),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call({add, Endpoint}, _From, State) ->
  lager:debug("Adding route: ~p -> ~p", [State#state.type, Endpoint]),
  reply(add2(Endpoint, State));
handle_call({remove, Endpoint}, _From, State) ->
  lager:debug("Removing route: ~p -> ~p", [State#state.type, Endpoint]),
  reply(remove2(Endpoint, State));
handle_call({replace, Endpoints}, _From, State) ->
  lager:debug("Replacing routes: ~p -> ~p", [State#state.type, Endpoints]),
  reply(replace2(Endpoints, State));
handle_call(next, _From, State) ->
  reply(next2(State));
handle_call(_Request, _From, State) ->
  reply({ok, State}).

handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Request, State) ->
  noreply(State).

handle_info(?SHOULD_CLEAN_CMD, State) ->
  noreply(should_clean(State));
handle_info(_Info, State) ->
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
    endpoints = gb_sets:new(),
    current = undefined
  }.

add2(Endpoint, State) ->
  {ok, State#state{
    endpoints = gb_sets:add_element(Endpoint, State#state.endpoints)
  }}.

remove2(Endpoint, State) ->
  send_cmd(?SHOULD_CLEAN_CMD),
  {ok, State#state{
    endpoints = gb_sets:delete_any(Endpoint, State#state.endpoints)
  }}.

replace2(Endpoints, State) ->
  send_cmd(?SHOULD_CLEAN_CMD),
  Routes2 = lists:foldl(
    fun(Endpoint, Routes) ->
      gb_sets:add_element(Endpoint, Routes)
    end,
    gb_sets:new(),
    Endpoints
  ),
  {ok, State#state{endpoints = Routes2}}.

next2(State) ->
  State2 = next3(State),
  case State2#state.current of
    undefined -> {undefined, State2};
    Endpoint -> {Endpoint, State2}
  end.

next3(State) ->
  case gb_sets:next(iter(State)) of
    {Endpoint, _} -> State#state{current = Endpoint};
    none -> State#state{current = undefined}
  end.

iter(State) -> %% return an iterator from current's next element
  Current = State#state.current,
  case Current =/= undefined of
    true ->
      Iter = gb_sets:iterator_from(Current, State#state.endpoints),
      case gb_sets:next(Iter) of
        %% current is still there
        {Current, Iter2} ->
          Largest = gb_sets:largest(State#state.endpoints),
          case Current =:= Largest of
            true -> gb_sets:iterator(State#state.endpoints);
            false -> Iter2
          end;
        %% current has been deleted
        {_, _} -> Iter;
        %% current is last element and has been deleted,
        %% so rewind the iterator
        none -> gb_sets:iterator(State#state.endpoints)
      end;
    false -> gb_sets:iterator(State#state.endpoints)
  end.

should_clean(State) ->
  case gb_sets:size(State#state.endpoints) =< 0 of
    true -> stop(self());
    false -> ignore
  end,
  State.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.

send_cmd(Cmd) ->
  erlang:send(self(), Cmd).