#!/bin/bash

echoinfo() { echo "[$(date -Is)] - $@" 1>&2; }

echoinfo "Starting test"
# These directories are used by other steps so make sure the non-root user has access
chown -R ubuntu:ubuntu /workspace/
chown -R ubuntu:ubuntu /cache/

# Need to switch users since we can't pass the right flag to allow running Cassandra as root
sudo -i -u ubuntu bash << EOF
echoinfo() { echo "[\$(date -Is)] - \$@" 1>&2; }
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin
export MAVEN_OPTS="-Dmaven.repo.local=/cache/.m2"

cd /workspace

# create temp directories to avoid issues with concurrent execution
cp -R . /tmp/$PERSISTENCE_BACKEND
cd /tmp/$PERSISTENCE_BACKEND
echoinfo "Copied directory"

C3="!"
C4="!"
DSE="!"
case "$PERSISTENCE_BACKEND" in
  "cassandra-3.11") C3=""  ;;
  "cassandra-4.0")  C4=""  ;;
  "dse-6.8")        DSE="" ;;
esac


echoinfo "Using backend $PERSISTENCE_BACKEND"

export CCM_CLUSTER_START_TIMEOUT_OVERRIDE=600
mvn -B failsafe:integration-test --file pom.xml \
-P \${C3}it-cassandra-3.11 \
-P \${C4}it-cassandra-4.0 \
-P \${DSE}dse -P \${DSE}it-dse-6.8 \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn

echoinfo "Test complete"
EOF


