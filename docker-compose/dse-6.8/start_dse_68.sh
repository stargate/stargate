#!/bin/sh

export SGTAG=v1.0.45

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

docker-compose up -d stargate restapi

# Wait until stargate is up before bringing up zeppelin

echo ""
echo "Waiting for stargate to start up..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8082/health)" != "200" ]]; do
    printf '.'
    sleep 5
done
