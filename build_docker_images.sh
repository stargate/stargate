#!/bin/bash
set -euo pipefail

export SGTAG=v2.0.0-ALPHA1

docker build --target sgv2-coordinator-4_0 -t stargateio/sgv2-coordinator-4_0:$SGTAG .
docker build --target sgv2-coordinator-3_11 -t stargateio/sgv2-coordinator-3_11:$SGTAG .
docker build --target sgv2-coordinator-dse-68 -t stargateio/sgv2-coordinator-dse-68:$SGTAG .
docker build --target sgv2-restapi -t stargateio/sgv2-restapi:$SGTAG .
