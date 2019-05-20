%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Jun 2018 6:13 PM
%%%-------------------------------------------------------------------
-module(maxwell_frontend_config).

%% API
-export([
  get_master_endpoints/0
]).

get_master_endpoints() ->
  {ok, MasterEndpoints} = application:get_env(
    maxwell_frontend, master_endpoints
  ),
  MasterEndpoints.