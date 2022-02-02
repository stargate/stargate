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

echo "Running Stargate version $SGTAG with DSE 6.8 (developer mode)"

# Can start all containers in parallel since DSE is running inside single Stargate coordinator
docker-compose -f docker-compose-dev-mode.yml up


