#!/usr/bin/env bash

set -euo pipefail

if [ -z ${1+x} ]; then
   echo "stargate version is a required argument"
   exit 1
fi

stargate_version=$1
DOCKER_IMAGE=stargateio/stargate-restapi

cd -P -- "$(dirname -- "$0")" # switch to this dir

echo "Building $DOCKER_IMAGE"
docker buildx build \
--tag ${DOCKER_IMAGE}:${stargate_version} \
--file Dockerfile \
.
#--platform linux/amd64,linux/arm64 .

#echo "Inspecting $DOCKER_IMAGE"
#docker buildx imagetools inspect ${DOCKER_IMAGE}:${stargate_version}