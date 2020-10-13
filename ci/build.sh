#!/bin/bash

apt-get update && apt-get install openjdk-8-jdk git python2.7 python-setuptools python-six python-yaml sudo maven -y

git clone --branch master --single-branch https://github.com/riptano/ccm.git

adduser --disabled-password --gecos "" ubuntu
chown -R ubuntu:ubuntu *

# Need to switch users since we can't pass the right flag to allow running Cassandra as root
sudo -i -u ubuntu bash << EOF
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin
cd /workspace

mvn -B install --file pom.xml \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
EOF