#!/bin/sh

# Set variable SGTAG

SGTAG="v$(../../mvnw -f ../.. help:evaluate -Dexpression=project.version -q -DforceStdout)"

while getopts ":pt:" opt; do
  case $opt in
    t)
      SGTAG=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

export SGTAG

echo "Running Stargate version $SGTAG with Cassandra 4.0 (developer mode)"

# Can start all containers in parallel since C* is running inside single Stargate coordinator
# Bring up the stargate: Coordinator first, then APIs

docker-compose -f docker-compose-dev-mode.yml up -d coordinator
(docker-compose logs -f coordinator &) | grep -q "Finished starting bundles"
docker-compose -f docker-compose-dev-mode.yml up -d restapi graphqlapi
