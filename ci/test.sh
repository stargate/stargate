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
chown -R ubuntu:ubuntu *

# Need to switch users since we can't pass the right flag to allow running Cassandra as root
sudo -i -u ubuntu bash << EOF
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
cd /workspace

#mkdir -p /tmp/ccm8202548329431989080
#ccm create ccm_1 -i 127.0.0. -n 1:0 -v 3.11.8 --config-dir=/tmp/ccm8202548329431989080
#ccm updateconf auto_snapshot:false --config-dir=/tmp/ccm8202548329431989080
#ccm updateconf cluster_name:Test_Cluster --config-dir=/tmp/ccm8202548329431989080
#ccm updateconf enable_user_defined_functions:true --config-dir=/tmp/ccm8202548329431989080
#ccm start --wait-for-binary-proto --config-dir=/tmp/ccm8202548329431989080 --verbose
#ccm checklogerror --config-dir=/tmp/ccm8202548329431989080
#
#
#for file in $(ls /tmp/ccm8202548329431989080/ccm_1/node1/logs/); do
#  echo "######## File: $file ########"
#  cat /tmp/ccm8202548329431989080/ccm_1/node1/logs/$file
#  echo ""
#  echo ""
#done

ls -altrh
pwd

export CCM_CLUSTER_START_TIMEOUT_OVERRIDE=600
./mvnw -B verify --file pom.xml \
-P it-cassandra-3.11 \
-P it-cassandra-4.0 \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
EOF


