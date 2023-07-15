#!/bin/bash
set -euo pipefail

# Script to generate Cassandra Docker image for Stargate based an experimental fork
# of the Apache Cassandra implementation. Assumes that you have done a complete build
# so that all jars have been created. You can download and build the fork using the
# download_cassandra.sh script at the root of the repository.

# Options:
# -p - causes the images to be built for all supported platform architectures and pushed to
#   Docker Hub (assumes you are logged in to Stargate Docker Hub account).
#   This is intended to be used as part of automated builds.
# -t <version> - overrides the default tag that will be applied to the image with the one
#   you provide. By default the tag consists of the version obtained from the coordinator
#   pom.xml file, i.e. 4.0.7-0acaae364c19.
# -r <repository> - overrides the default repository used to tag image. By
#   default the repository is "stargateio" (Docker Hub).

#
# Defaults
#

# generate for local platform, don't push
DOCKER_FLAGS="--load"

# extract Stargate version from project pom file
CASSTAG="$(cd ../coordinator; ./mvnw -pl persistence-cassandra-5.0 help:evaluate -Dexpression=cassandra.version -q -DforceStdout)"

REPO="stargateio"

CASSANDRA_DIR=cassandra

#
# overrides via command line
#

while getopts ":t:r:d:p" opt; do
  case $opt in
    p)
      DOCKER_FLAGS="--platform linux/amd64,linux/arm64 --push"
      ;;
    t)
      CASSTAG=$OPTARG
      ;;
    r)
      REPO="$OPTARG"
      ;;
    d)
      CASSANDRA_DIR="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

echo "Building docker image $REPO/cassandra-5.0:$CASSTAG using cassandra from $CASSANDRA_DIR"

# Verify existence of cassandra jars
if [ ! -f $CASSANDRA_DIR/build/dse-db*.jar ]; then
  echo "Cassandra jars are not present in build directory, e.g. "
  echo "no files matching $CASSANDRA_DIR/build/dse-db*.jar found"
  echo "Make sure to build distribution using the download_cassandra.sh script"
  exit 1
fi

docker buildx build --build-arg CASSANDRA_DIR=$CASSANDRA_DIR -t $REPO/cassandra-5_0:$CASSTAG $DOCKER_FLAGS .
