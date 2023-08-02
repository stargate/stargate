#!/bin/bash
set -euo pipefail

# This script is used to download a fork of Apache Cassandra including additional
# vector search and SAI indexing features that have been proposed for Cassandra 5
# via the Cassandra Enhancement Proposal (CEP) process.
#

git clone -b vsearch https://github.com/datastax/cassandra.git cassandra

cd cassandra/
ant -Duse.jdk11=true

# Access to this directory is required in order for this version of Cassandra
# to run using ccm as required for integration tests
# see the coordinator/persistence-dse-next/README.md for more details
sudo mkdir -p /var/log/cassandra
sudo chmod 777 /var/log/cassandra
