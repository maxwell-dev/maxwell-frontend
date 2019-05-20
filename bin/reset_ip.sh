#!/usr/bin/env bash

root_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )";
dev_config="${root_dir}/config/vm.dev.args";
prod_config="${root_dir}/config/vm.prod.args";

function reset_public_ip() {
    file=$1;
    os=$(uname);
    if [[ "${os}" = "Linux" ]]; then
        sed -i "s%public_ip[[:space:]]*.*%public_ip 0.0.0.0%g" ${file};
    elif [[ "${os}" == "Darwin" ]]; then
        sed -i "" "s%public_ip[[:space:]]*.*%public_ip 0.0.0.0%g" ${file};
    fi
}

reset_public_ip ${dev_config};
reset_public_ip ${prod_config};