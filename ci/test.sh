#!/bin/bash

# These directories are used by other steps so make sure the non-root user has access
chown -R ubuntu:ubuntu /workspace/
chown -R ubuntu:ubuntu /cache/

# Need to switch users since we can't pass the right flag to allow running Cassandra as root
sudo -i -u ubuntu bash << EOF
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin
export MAVEN_OPTS="-Dmaven.repo.local=/cache/.m2"

cd /workspace

# create temp directories to avoid issues with concurrent execution
cp -R . /tmp/$PERSISTENCE_BACKEND
cd /tmp/$PERSISTENCE_BACKEND

PROFILE=""
case "$PERSISTENCE_BACKEND" in
  "cassandra-3.11") PROFILE="it-cassandra-3.11"  ;;
  "cassandra-4.0")  PROFILE="it-cassandra-4.0"  ;;
  "dse-6.8")        PROFILE="dse,it-dse-6.8" ;;
esac

echo "Using backend $PERSISTENCE_BACKEND"

export CCM_CLUSTER_START_TIMEOUT_OVERRIDE=600
mvn -B failsafe:integration-test --file pom.xml -P "${PROFILE}" \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
EOF


