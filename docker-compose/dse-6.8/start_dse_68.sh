#!/bin/bash

# require Docker Compose v2
if [[ ! $(docker compose version --short) =~ ^2. ]]; then
  echo "Docker compose v2 required. Please upgrade Docker Desktop to the latest version."
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO
# Default to using images tagged "v2"
SGTAG=v2

while getopts "lqr:t:" opt; do
  case $opt in
    l)
      pushd ../../coordinator > /dev/null
      SGTAG="v$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)"
      popd > /dev/null
      ;;
    q)
      REQUESTLOG=true
      ;;
    r)
      LOGLEVEL=$OPTARG
      ;;
    t)
      SGTAG=$OPTARG
      ;;
    \?)
      echo "Valid options:"
      echo "  -t <tag> - use Docker images tagged with specified Stargate version (will pull images from Docker Hub if needed)"
      echo "  -l - use Docker images from local build (see project README for build instructions)"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export SGTAG

echo "Running Stargate version $SGTAG with DSE 6.8"

docker-compose up -d --wait