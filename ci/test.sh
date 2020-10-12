#!/bin/bash


apt-get update && apt-get install openjdk-8-jdk git python2.7 python-setuptools python-six python-yaml sudo -y
java -version

ln -sf /usr/bin/python2.7 /usr/bin/python

export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"

git clone --branch master --single-branch https://github.com/riptano/ccm.git
pushd ccm || exit
sudo python setup.py install
popd

adduser --disabled-password --gecos "" ubuntu
su - ubuntu

export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"

CCM_CLUSTER_START_TIMEOUT_OVERRIDE=600 ./mvnw -B verify --file pom.xml \
-P it-cassandra-3.11 \
-P it-cassandra-4.0 \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn