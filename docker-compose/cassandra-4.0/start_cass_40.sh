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

echo "Running Stargate version $SGTAG with Cassandra 4.0"

# Make sure cassandra-1, the seed node, is up before bringing up other nodes and stargate

docker-compose up -d cassandra-1

# Wait until the seed node is up before bringing up more nodes

(docker-compose logs -f cassandra-1 &) | grep -q "Created default superuser role"

# Bring up the 2nd C* node

docker-compose up -d cassandra-2
(docker-compose logs -f cassandra-2 &) | grep -q "Startup complete"

# Bring up the 3rd C* node

docker-compose up -d cassandra-3
(docker-compose logs -f cassandra-3 &) | grep -q "Startup complete"

# Bring up the stargate: Coordinator first, then APIs

docker-compose up -d coordinator
(docker-compose logs -f coordinator &) | grep -q "Finished starting bundles"
docker-compose up -d restapi graphqlapi docsapi
