#!/bin/bash
set -euo pipefail

# Script to generate Docker images for Stargate
# Assumes that you have done a complete build so that all jars have been created, i.e.:
#   ./mvnw clean install -P dse -DskipTests=true

export SGTAG=v2.0.0-ALPHA1

# Create a temp directory under current directory to use as a staging area for image creation
# This is a workaround since Docker COPY command provides no way to exclude files.
# We could build a base image and then remove files, but this is wasteful
# Note: Docker will not recognize files that are not under current directory
LIBDIR=./tmp-${RANDOM}
mkdir ${LIBDIR}
cp ./stargate-lib/* $LIBDIR
rm ${LIBDIR}/persistence*.jar

#ls ${LIBDIR}

docker build --target coordinator-4_0 --build-arg LIBDIR="$LIBDIR" -t stargateio/coordinator-4_0:$SGTAG .
docker build --target coordinator-3_11 --build-arg LIBDIR="$LIBDIR" -t stargateio/coordinator-3_11:$SGTAG .
docker build --target coordinator-dse-68 --build-arg LIBDIR="$LIBDIR" -t stargateio/coordinator-dse-68:$SGTAG .

rm -rf ${LIBDIR}

docker build --target restapi -t stargateio/restapi:$SGTAG .

