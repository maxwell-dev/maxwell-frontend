%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2019 17:03
%%%-------------------------------------------------------------------
-module(maxwell_frontend_route_syncer).
-behaviour(gen_server).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").
-include_lib("maxwell_client/include/maxwell_client.hrl").

%% API
-export([
  start_link/0
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
-define(PUSH_CMD, '$push').
-define(PULL_CMD, '$pull').

-record(state, {connector_ref, connected = false}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  Ref = erlang:monitor(process, maxwell_frontend_master_connector:get_pid()),
  State = #state{connector_ref = Ref},
  maxwell_frontend_master_connector:add_listener(self()),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  send_cmd(?PUSH_CMD, 5000),
  send_cmd(?PULL_CMD, 10000),
  {ok, State}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(?ON_CONNECTED_CMD(Pid), State) ->
  lager:debug("ON_CONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_connected(State)};
handle_info(?ON_DISCONNECTED_CMD(Pid), State) ->
  lager:debug("ON_DISCONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_disconnected(State)};
handle_info(?PUSH_CMD, State) ->
  {noreply, push(State)};
handle_info(?PULL_CMD, State) ->
  {noreply, pull(State)};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) 
    when Ref =:= State#state.connector_ref ->
  {stop, {shutdown, connector_was_down}, State};
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
  State#state{connected = true}.

on_disconnected(State) ->
  State#state{connected = false}.

push(State) ->
  case State#state.connected of
    true -> push_routes();
    false -> ignore
  end,
  send_cmd(?PUSH_CMD, 5000),
  State.

push_routes() ->
  Req = #push_routes_req_t{types = maxwell_frontend_watcher_mgr_mgr:get_types()},
  case maxwell_frontend_master_connector:send(Req, 5000) of
    #push_routes_rep_t{} -> ignore;
    {error, {_, timeout}} -> ignore;
    Error -> lager:error("Failed to push routes: ~p", [Error])
  end.

pull(State) ->
  case State#state.connected of
    true -> pull_routes();
    false -> ignore
  end,
  send_cmd(?PULL_CMD, 10000),
  State.

pull_routes() ->
  case maxwell_frontend_master_connector:send(#pull_routes_req_t{}, 5000) of
    #pull_routes_rep_t{route_groups = RouteGroups} -> sync_routes(RouteGroups);
    {error, {_, timeout}} -> ignore;
    Error -> lager:error("Failed to pull routes: ~p", [Error])
  end.

sync_routes(RouteGroups) ->
  Types2 = lists:foldl(fun(RouteGroup, Types) ->
      #route_group_t{type = Type, endpoints = Endpoints} = RouteGroup,
      maxwell_frontend_route_mgr:replace(Type, Endpoints),
      sets:add_element(Type, Types)
    end, sets:new(), RouteGroups
  ),
  lists:foreach(fun(Type) -> 
      case sets:is_element(Type, Types2) of
        true -> ignore;
        false -> maxwell_frontend_route_mgr:stop(Type)
      end
    end, maxwell_frontend_route_mgr_mgr:get_types()
  ).

send_cmd(Cmd, DelayMS) ->
  erlang:send_after(DelayMS, self(), Cmd).