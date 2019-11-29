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
-include_lib("maxwell_client/include/maxwell_client.hrl").

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
-define(REGISTER_CMD, '$register').

-record(state, {
  connector_ref,
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
  Ref = erlang:monitor(process, maxwell_frontend_master_connector:get_pid()),
  State = #state{connector_ref = Ref, registered = false},
  maxwell_frontend_master_connector:add_listener(self()),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call(_Request, _From, State) ->
  {stop, received_unknown_call, State}.

handle_cast(_Request, State) ->
  {stop, received_unknown_cast, State}.

handle_info(?ON_CONNECTED_CMD(Pid), State) ->
  lager:debug("ON_CONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_connected(State)};
handle_info(?ON_DISCONNECTED_CMD(Pid), State) ->
  lager:debug("ON_DISCONNECTED_CMD: pid: ~p", [Pid]),
  {noreply, on_disconnected(State)};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) 
    when Ref =:= State#state.connector_ref ->
  {stop, {shutdown, connector_was_down}, State};
handle_info(_Info, State) ->
  {stop, received_unknown_info, State}.

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
  Req = #register_frontend_req_t{endpoint = build_public_endpoint()},
  lager:info("Registering frontend: ~p", [Req]),
  Rep = maxwell_frontend_master_connector:send(Req, 5000),
  handle_response(Rep, State).

handle_response(#register_frontend_rep_t{}, State) ->
  lager:info("Frontend was successfully registered!", []),
  State#state{registered = true};
handle_response(#error_rep_t{code = _, desc = _} = Rep, State) ->
  lager:info("Failed to register frontend: ~p", [Rep]),
  send_cmd(?REGISTER_CMD, 1000),
  State#state{registered = false};
handle_response({error, {_, _}} = Error, State) ->
  lager:info("Failed to register frontend: ~p", [Error]),
  send_cmd(?REGISTER_CMD, 1000),
  State#state{registered = false}.

build_public_endpoint() ->
  {ok, Ip} = maxwell_ip_resolver:resolve(),
  lists:concat([binary_to_list(Ip), ":", maxwell_server_config:get_port()]).

send_cmd(Cmd, DelayMS) ->
  erlang:send_after(DelayMS, self(), Cmd).