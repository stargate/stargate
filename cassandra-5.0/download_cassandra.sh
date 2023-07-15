#!/bin/bash
set -euo pipefail

# This script is used to download an experimental fork of Cassandra implementation
# and may include code that has not yet been accepted into the Apache Cassandra project.
#

git clone -b vsearch https://github.com/datastax/cassandra.git cassandra

cd cassandra/
ant -Duse.jdk11=true

# Access to this directory is required in order for this version of Cassandra
# to run using ccm as required for integration tests
# see the coordinator/persistence-cassandra-5.0/README.md for more details
sudo mkdir -p /var/log/cassandra
sudo chmod 777 /var/log/cassandra
