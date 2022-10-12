#!/bin/sh

# Default to using images tagged "v2"
SGTAG=v2

while getopts "lt:" opt; do
  case $opt in
    t)
      SGTAG=$OPTARG
      ;;
    l)
      SGTAG="v$(../../mvnw -f ../.. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    \?)
      echo "Valid options:"
      echo "  -t <tag> - use Docker images tagged with specified Stargate version (will pull images from Docker Hub if needed)"
      echo "  -l - use Docker images from local build (see project README for build instructions)"
      exit 1
      ;;
  esac
done

export SGTAG

echo "Running Stargate version $SGTAG with DSE 6.8"

# Make sure dse-1, the seed node, is up before bringing up other nodes and stargate

docker-compose up -d dse-1

# Wait until the seed node is up before bringing up more nodes

(docker-compose logs -f dse-1 &) | grep -q "Created default superuser role"

# Bring up the 2nd C* node

docker-compose up -d dse-2
(docker-compose logs -f dse-2 &) | grep -q "is now part of the cluster"

# Bring up the 3rd C* node

docker-compose up -d dse-3
(docker-compose logs -f dse-3 &) | grep -q "is now part of the cluster"

# Bring up the stargate: Coordinator first, then APIs

docker-compose up -d coordinator
(docker-compose logs -f coordinator &) | grep -q "Finished starting bundles"
docker-compose up -d restapi graphqlapi docsapi
