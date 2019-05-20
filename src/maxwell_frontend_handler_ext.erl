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
  pre_pull/2,
  pre_push/2,
  handle/2,
  terminate/2
]).

-record(state, {
  initial_req,
  handler_pid,
  handler_id,
  token,
  node_id
}).

%%%===================================================================
%%% Server callbacks
%%%===================================================================

init(Req) ->
  HandlerPid = self(),
  State = #state{
    initial_req = Req,
    handler_pid = HandlerPid,
    handler_id = maxwell_frontend_handler_id_mgr:assign_id(HandlerPid),
    node_id = maxwell_server_config:get_node_id()
  },
  lager:debug("Initializing handler_ext: state: ~p", [State]),
  State.

pre_pull(#pull_req_t{topic = Topic}, State) ->
  case State#state.token =/= undefined of
    true ->
      maxwell_frontend_puller_mgr:ensure_puller_started(Topic),
      ok;
    false ->
      not_allowed_to_pull
  end.

pre_push(_, State) ->
  case State#state.token =/= undefined of
    true -> ok;
    false -> not_allowed_to_push
  end.

handle(#auth_req_t{token = Token, ref = Ref}, State) ->
  reply(#auth_rep_t{ref = Ref}, State#state{token = Token});
handle(#watch_req_t{type = Type, ref = Ref}, State) ->
  ok = maxwell_frontend_watcher_mgr:add(Type, State#state.handler_pid),
  reply(#watch_rep_t{ref = Ref}, State);
handle(#unwatch_req_t{type = Type, ref = Ref}, State) ->
  ok = maxwell_frontend_watcher_mgr:remove(Type, State#state.handler_pid),
  reply(#unwatch_rep_t{ref = Ref}, State);
handle(#do_req_t{type = Type} = Msg, State) ->
  Msg2 = ensure_source_built(fill_trace(Msg, State), State),
  WatcherPid = maxwell_frontend_watcher_mgr:next(Type),
  case WatcherPid =/= undefined of
    true -> maxwell_server_handler:send(WatcherPid, Msg2);
    false ->
      Endpoint = maxwell_frontend_route_mgr:next(Type),
      case Endpoint =/= undefined of
        true ->
          {ok, ConnPid} = maxwell_client_conn_mgr:fetch(Endpoint),
          maxwell_client_conn:send(ConnPid, Msg2, 5000);
        false ->
          lager:warning("Cannot find available watcher: type: ~p", [Type])
      end
  end,
  noreply(State);
handle(#do_rep_t{traces = [Trace | _]} = Msg, State) ->
  send(Trace#trace_t.handler_id, Msg),
  noreply(State);
handle(Msg, State) ->
  lager:error("Received unknown msg: ~p", [Msg]),
  noreply(State).

terminate(Reason, State) ->
  lager:debug(
    "Terminating handler_ext: reason: ~p, state: ~p", [Reason, State]
  ),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

send(HandlerId, Msg) ->
  maxwell_server_handler:send(
    maxwell_frontend_handler_id_mgr:get_pid(HandlerId), Msg
  ).

fill_trace(#do_req_t{traces = [Trace | RestTraces]} = Msg, State) ->
  Trace2 = Trace#trace_t{
    handler_id = State#state.handler_id,
    node_id = State#state.node_id
  },
  Msg#do_req_t{traces = [Trace2 | RestTraces]}.

ensure_source_built(#do_req_t{
  source_enabled = SourceEnabled, source = Source
} = Msg, State) ->
  case Source =/= undefined of
    true -> Msg;
    false ->
      case SourceEnabled of
        true -> Msg#do_req_t{
          source = build_source(State#state.initial_req)
        };
        false -> Msg
      end
  end.

build_source(InitialReq) ->
  #source_t{
    agent = io_lib:format("~p", [maps:get(agent, InitialReq)]),
    endpoint = io_lib:format("~p", [maps:get(endpoint, InitialReq)])
  }.

reply(Reply, State) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.

%%stop(Reason, State) ->
%%  {stop, Reason, State}.