%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Jun 2018 5:35 PM
%%%-------------------------------------------------------------------
-module(maxwell_frontend_handler_ext).

-include_lib("maxwell_protocol/include/maxwell_protocol_pb.hrl").

-export([
  init/1,
  handle/2,
  terminate/2
]).

-record(state, {
  initial_req,
  node_id,
  handler_pid,
  handler_id,
  endpoint_conns,
  conn_endpoints
}).

%%%===================================================================
%%% Server callbacks
%%%===================================================================

init(Req) ->
  State = init_state(Req),
  lager:debug("Initializing ~p: state: ~p", [?MODULE, State]),
  State.

handle(#watch_req_t{type = Type, ref = Ref}, State) ->
  ok = maxwell_frontend_watcher_mgr:add(Type, State#state.handler_pid),
  reply(#watch_rep_t{ref = Ref}, State);

handle(#unwatch_req_t{type = Type, ref = Ref}, State) ->
  ok = maxwell_frontend_watcher_mgr:remove(Type, State#state.handler_pid),
  reply(#unwatch_rep_t{ref = Ref}, State);

handle(#pull_req_t{topic = Topic, ref = Ref} = Req, State) ->
  case maxwell_backend_resolver:resolve(Topic) of
    {ok, Endpoint} ->
      {ok, ConnPid, State2} = fetch_conn(Endpoint, State),
      send_to_backend(ConnPid, set_puller(Req, State2)),
      noreply(State2);
    Error ->
      lager:error("Failed to find backend: topic: ~p", [Topic]),
      reply(build_error_rep(Error, Ref), State)
  end;

handle(#pull_rep_t{} = PullRep, State) ->
  reply(PullRep, State);

handle(#do_req_t{} = Req, State) ->
  Req2 = set_source(set_handler_id(Req, State), State),
  handle({retry, Req2, ok, 0}, State);
handle({retry, #do_req_t{type = Type} = Req, _, Count}, State)
when Count < 3 ->
  case try_send_to_watcher(Type, Req) of
    ok -> noreply(State);
    {error, failed_to_find_watcher} ->
      case try_send_to_route(Type, Req, State) of
        {ok , State2} -> noreply(State2);
        {error, Reason} ->
          Delay = (Count + 1) * 500,
          Cmd = {retry, Req, Reason, Count + 1},
          lager:debug("Will retry: cmd: ~p, delay: ~p", [Cmd, Delay]),
          erlang:send_after(Delay, self(), Cmd),
          noreply(State)
      end
  end;
handle({retry, #do_req_t{type = Type} = Req, Reason, 3}, State) ->
  lager:error("Failed to find watcher: reason: ~p, type: ~p", [Reason, Type]),
  Error = build_error2_rep(
    {error, {failed_to_find_watcher, Reason, Type}}, Req#do_req_t.traces
  ),
  reply(Error, State);

handle(#do_rep_t{traces = Traces} = Rep, State) ->
  Traces2 = [Trace | _] = drop_trace(Traces, State),
  send_to_client(
    maxwell_frontend_handler_id_mgr:get_pid(Trace#trace_t.handler_id),
    Rep#do_rep_t{traces = Traces2}
  ),
  noreply(State);

handle(#ok2_rep_t{traces = Traces} = Rep, State) ->
  Traces2 = [Trace | _] = drop_trace(Traces, State),
  send_to_client(
    maxwell_frontend_handler_id_mgr:get_pid(Trace#trace_t.handler_id),
    Rep#ok2_rep_t{traces = Traces2}
  ),
  noreply(State);

handle(#error2_rep_t{traces = Traces} = Rep, State) ->
  Traces2 = [Trace | _] = drop_trace(Traces, State),
  send_to_client(
    maxwell_frontend_handler_id_mgr:get_pid(Trace#trace_t.handler_id),
    Rep#error2_rep_t{traces = Traces2}
  ),
  noreply(State);

handle({error, Reason, Ref}, State) ->
  reply(build_error_rep(Reason, Ref), State);

handle({'DOWN', ConnRef, process, _ConnPid, _Reason}, State) ->
  noreply(release_conn(ConnRef, State));

handle(Msg, State) ->
  lager:error(
    "Received unknown msg: ~p, initial_req: ~p", [Msg, State#state.initial_req]
  ),
  noreply(State).

terminate(Reason, State) ->
  lager:debug("Terminating handler_ext: reason: ~p, state: ~p", [Reason, State]),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_state(Req) ->
  HandlerPid = self(),
  #state{
    initial_req = Req,
    node_id = get_node_id(),
    handler_pid = HandlerPid,
    handler_id = maxwell_frontend_handler_id_mgr:assign_id(HandlerPid),
    endpoint_conns = dict:new(),
    conn_endpoints = dict:new()
  }.

get_node_id() ->
  {ok, Ip} = maxwell_ip_resolver:resolve(),
  IpParts = binary:split(Ip, <<".">>, [global]),
  Ip0 = binary_to_integer(lists:nth(1, IpParts)),
  Ip1 = binary_to_integer(lists:nth(2, IpParts)),
  Ip2 = binary_to_integer(lists:nth(3, IpParts)),
  Ip3 = binary_to_integer(lists:nth(4, IpParts)),
  Port = maxwell_server_config:get_port(),
  <<
    Ip0:8/little-unsigned-integer-unit:1,
    Ip1:8/little-unsigned-integer-unit:1,
    Ip2:8/little-unsigned-integer-unit:1,
    Ip3:8/little-unsigned-integer-unit:1,
    Port:16/little-unsigned-integer-unit:1
  >>.

fetch_conn(Endpoint, State) ->
  case dict:find(Endpoint, State#state.endpoint_conns) of
    {ok, {_, ConnPid}} -> {ok, ConnPid, State};
    error ->
      {ok, ConnPid} = maxwell_client_conn_pool:fetch(Endpoint),
      ConnRef = erlang:monitor(process, ConnPid),
      {ok, ConnPid, register_endpoint(Endpoint, {ConnRef, ConnPid}, State)}
  end.

release_conn(ConnRef, State) ->
  erlang:demonitor(ConnRef),
  unregister_endpoint(ConnRef, State).

register_endpoint(Endpoint, {ConnRef, _} = Conn, State) -> 
  EndpointConns = dict:store(Endpoint, Conn, State#state.endpoint_conns),
  ConnEndpoints = dict:store(ConnRef, Endpoint, State#state.conn_endpoints),
  State#state{endpoint_conns = EndpointConns, conn_endpoints = ConnEndpoints}.

unregister_endpoint(ConnRef, State) -> 
  case dict:find(ConnRef, State#state.conn_endpoints) of
    {ok, Endpoint} ->
      EndpointConns = dict:erase(Endpoint, State#state.endpoint_conns),
      ConnEndpoints = dict:erase(ConnRef, State#state.conn_endpoints),
      State#state{endpoint_conns = EndpointConns, conn_endpoints = ConnEndpoints};
    error -> State
  end.

send_to_backend(ConnPid, #pull_req_t{ref = Ref} = Req) ->
  catch maxwell_client_conn:async_send(
    ConnPid, 
    Req, 
    5000, 
    fun(Rep)->
      case Rep of
        #pull_rep_t{} -> Rep#pull_rep_t{ref = Ref};
        {error, Reason, _} -> {error, Reason, Ref};
        {error, Reason} -> {error, Reason, Ref}
      end
    end
  ).

try_send_to_watcher(Type, Req) ->
  WatcherPid = maxwell_frontend_watcher_mgr:next(Type),
  case WatcherPid =/= undefined of
    true ->
      send_to_watcher(WatcherPid, Req),
      ok;
    false -> {error, failed_to_find_watcher}
    end.

try_send_to_route(Type, Req, State) ->
  case erlang:length(Req#do_req_t.traces) < 2 of
    true ->
      Endpoint = maxwell_frontend_route_mgr:next(Type),
      case Endpoint =/= undefined of
        true ->
          {ok, ConnPid, State2} = fetch_conn(Endpoint, State),
          send_to_route(ConnPid, add_trace(Req, State2)),
          {ok, State2};
        false -> {error, failed_to_find_route}
      end;
    false -> {error, prevent_more_routes}
  end.

send_to_client(HandlerPid, Rep) -> 
  maxwell_server_handler:send(HandlerPid, Rep).

send_to_watcher(WatcherPid, Req) ->
  maxwell_server_handler:send(WatcherPid, Req).

send_to_route(ConnPid, Req) ->
  #trace_t{ref = Ref} = lists:nth(2, Req#do_req_t.traces),
  catch maxwell_client_conn:async_send(
    ConnPid,
    Req,
    5000,
    fun(Rep)->
      case Rep of
        {error, Reason, _} -> {error, Reason, Ref};
        {error, Reason} -> {error, Reason, Ref};
        Rep -> Rep
      end
    end
  ).

set_puller(Req, State) ->
  Req#pull_req_t{puller = State#state.handler_id}.

set_handler_id(#do_req_t{traces = [Trace | RestTraces]} = Req, State) -> 
  Req#do_req_t{
    traces = [Trace#trace_t{handler_id = State#state.handler_id} | RestTraces]
  }.

set_source(#do_req_t{
  source_enabled = SourceEnabled, source = Source
} = Req, State) ->
  case Source =/= undefined of
    true -> Req;
    false ->
      case SourceEnabled of
        true -> Req#do_req_t{source = build_source(State#state.initial_req)};
        false -> Req
      end
  end.

build_source(InitialReq) ->
  #source_t{
    agent = io_lib:format("~p", [maps:get(agent, InitialReq)]),
    endpoint = io_lib:format("~p", [maps:get(endpoint, InitialReq)])
  }.

add_trace(#do_req_t{traces = Traces} = Req, State) -> 
  Req#do_req_t{traces = [#trace_t{node_id = State#state.node_id} | Traces]}.

drop_trace([#trace_t{node_id = NodeId} | RestTraces] = Traces, State) ->
  case NodeId =:= State#state.node_id of
    true -> RestTraces;
    false -> Traces
  end.

build_error_rep(Error, Ref) ->
  #error_rep_t{
    code = 1, desc = io_lib:format("~p", [Error]), ref = Ref
  }.

build_error2_rep(Error, Traces) ->
  #error2_rep_t{
    code = 1, desc = io_lib:format("~p", [Error]), traces = Traces
  }.

reply(Reply, State) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.