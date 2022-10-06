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

echo "Running Stargate version $SGTAG with DSE 6.8 (developer mode)"

# Can start all containers in parallel since DSE is running inside single Stargate coordinator
# Bring up the stargate: Coordinator first, then APIs

docker-compose -f docker-compose-dev-mode.yml up -d coordinator
(docker-compose logs -f coordinator &) | grep -q "Finished starting bundles"
docker-compose -f docker-compose-dev-mode.yml up -d restapi graphqlapi docsapi
