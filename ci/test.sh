#!/bin/bash

apt-get update && apt-get install openjdk-8-jdk git python2.7 -y
java -version

git clone --branch master --single-branch https://github.com/riptano/ccm.git
pushd ccm || exit
python2.7 setup.py install
popd


CCM_CLUSTER_START_TIMEOUT_OVERRIDE=600 ./mvnw -B verify --file pom.xml -P it-cassandra-3.11 -P it-cassandra-4.0