#!/bin/bash
set -e

cleanup() {
    # kill all processes whose parent is this process
    pkill -P $$
}

for sig in INT QUIT HUP TERM; do
  trap "
    cleanup
    trap - $sig EXIT
    kill -s $sig "'"$$"' "$sig"
done
trap cleanup EXIT

if [ -z $BRIDGE_TOKEN ]; then
  export BRIDGE_TOKEN=mockAdminToken
fi

# start REST API service
./starctl-service-rest &

# pass along same arguments to script to start coordinator
./starctl "$@" --bridge-token="$BRIDGE_TOKEN"

