#!/bin/bash

apt-get update && apt-get install openjdk-8-jdk git python2.7 python-setuptools python-six python-yaml sudo maven -y

update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin
java -version

ln -sf /usr/bin/python2.7 /usr/bin/python

#git clone --branch master --single-branch https://github.com/riptano/ccm.git
pushd ccm || exit
sudo python setup.py install
popd

adduser --disabled-password --gecos "" ubuntu
chown -R ubuntu:ubuntu *

# Need to switch users since we can't pass the right flag to allow running Cassandra as root
sudo -i -u ubuntu bash << EOF
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin
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
mvn -B verify --file pom.xml -P "${PROFILE}" \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
EOF


