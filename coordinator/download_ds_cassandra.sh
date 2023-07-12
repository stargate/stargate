#!/bin/bash
set -euo pipefail

# Script to download Datastax Cassandra distribution used for testing persistence-cassandra-5.0 module
git clone https://github.com/datastax/cassandra.git ds-cassandra
cd ds-cassandra
ant -Duse.jdk11=true

