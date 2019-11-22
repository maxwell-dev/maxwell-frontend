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
  Req = #push_routes_req_t{
    types = maxwell_frontend_watcher_mgr_mgr:get_types()
  },
  Rep = maxwell_frontend_master_connector:send(Req, 5000),
  end_push_routes(Rep).

end_push_routes(#push_routes_rep_t{}) ->
  ignore;
end_push_routes({error, {_, timeout}}) ->
  ignore;
end_push_routes(Error) ->
  lager:error("Failed to push routes: ~p", [Error]).

pull(State) ->
  case State#state.connected of
    true -> pull_routes();
    false -> ignore
  end,
  send_cmd(?PULL_CMD, 10000),
  State.

pull_routes() ->
  Req = #pull_routes_req_t{},
  Rep = maxwell_frontend_master_connector:send(Req, 5000),
  end_pull_routes(Rep).

end_pull_routes(#pull_routes_rep_t{route_groups = RouteGroups}) ->
  lists:foreach(
    fun(RouteGroup) ->
      maxwell_frontend_route_mgr:replace(
        RouteGroup#route_group_t.type,
        RouteGroup#route_group_t.endpoints
      )
    end,
    RouteGroups
  );
end_pull_routes({error, {_, timeout}}) ->
  ignore;
end_pull_routes(Error) ->
  lager:error("Failed to pull routes: ~p", [Error]).

send_cmd(Cmd, DelayMS) ->
  erlang:send_after(DelayMS, self(), Cmd).