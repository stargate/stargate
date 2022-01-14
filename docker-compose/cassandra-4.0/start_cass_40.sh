#!/bin/sh

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

# Bring up the stargate

docker-compose up -d stargate stargate-restapi

# Wait until stargate is up before bringing up zeppelin

echo ""
echo "Waiting for stargate to start up..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8082/health)" != "200" ]]; do
    printf '.'
    sleep 5
done
