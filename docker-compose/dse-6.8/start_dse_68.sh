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

# Bring up the stargate

docker-compose up -d coordinator restapi graphqlapi docsapi
