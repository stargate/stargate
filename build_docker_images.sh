#!/bin/bash
set -euo pipefail

export SGTAG=v2.0.0-ALPHA1

docker build --target coordinator-4_0 -t stargateio/coordinator-4_0:$SGTAG .
docker build --target coordinator-3_11 -t stargateio/coordinator-3_11:$SGTAG .
docker build --target coordinator-dse-68 -t stargateio/coordinator-dse-68:$SGTAG .
docker build --target restapi -t stargateio/restapi:$SGTAG .
