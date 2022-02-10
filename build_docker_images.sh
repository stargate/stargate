#!/bin/bash
set -euo pipefail

# Script to generate Docker images for Stargate
# Assumes that you have done a complete build so that all jars have been created, i.e.:
#   ./mvnw clean install -P dse -DskipTests=true

# Options:
# -p - causes the images to be built for all supported platform architectures and pushed to
#   Docker Hub (assumes you are logged in to Stargate Docker Hub account).
#   This is intended to be used as part of automated builds.
# -t <version> - overrides the default tag that will be applied to the image with the one
#   you provide. By default the tag consists of the version is obtained from the parent
#   pom.xml file, prepended with v, i.e. v2.0.0.

#
# Defaults
#

# generate for local platform, don't push
DOCKER_FLAGS="--load"

# extract Stargate version from project pom file
SGTAG="v$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

#
# overrides via command line
#

while getopts ":pt:" opt; do
  case $opt in
    p)
      DOCKER_FLAGS="--platform linux/amd64,linux/arm64 --push"
      ;;
    t)
      SGTAG=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

echo "Building version $SGTAG"

#
# Persistence images
#

# Create a temp directory under current directory to use as a staging area for image creation
# This is a workaround since Docker COPY command provides no way to exclude files.
# We could build a base image and then remove files, but this is wasteful
# Note: Docker will not recognize files that are not under current directory
LIBDIR=./tmp-${RANDOM}
mkdir ${LIBDIR}
cp ./stargate-lib/*.jar $LIBDIR
cp ./stargate-lib/logback.xml $LIBDIR
rm ${LIBDIR}/persistence*.jar

docker buildx build --target coordinator-4_0 --build-arg LIBDIR="$LIBDIR" -t stargateio/coordinator-4_0:$SGTAG $DOCKER_FLAGS .
docker buildx build --target coordinator-3_11 --build-arg LIBDIR="$LIBDIR" -t stargateio/coordinator-3_11:$SGTAG $DOCKER_FLAGS .
docker buildx build --target coordinator-dse-68 --build-arg LIBDIR="$LIBDIR" -t stargateio/coordinator-dse-68:$SGTAG $DOCKER_FLAGS .

rm -rf ${LIBDIR}

#
# API Service images
#

docker buildx build --target restapi -t stargateio/restapi:$SGTAG $DOCKER_FLAGS .

