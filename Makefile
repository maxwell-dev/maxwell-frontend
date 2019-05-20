.PHONY : default compile release-dev release-prod run test clean

REBAR=rebar3

default: run

compile:
	${REBAR} get-deps
	${REBAR} compile

release-dev: compile
	bin/ensure_ip_resolved.sh
	${REBAR} release -n maxwell_frontend_dev

release-prod: compile
	bin/ensure_ip_resolved.sh
	${REBAR} release -n maxwell_frontend_prod

run: release-dev
	_build/default/rel/maxwell_frontend_dev/bin/maxwell_frontend_dev console

test:
	ERL_FLAGS="-args_file config/vm.dev.args" ${REBAR} eunit

clean:
	${REBAR} clean
