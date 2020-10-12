#!/bin/bash

mvn -B verify --settings ci/mvn-settings.xml --file pom.xml \
-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
-Dstargate.logging.level.root=WARN \
-Dstargate.logging.level.cassandra=WARN \
-Dstargate.logging.level.dse=WARN \
-P it-cassandra-3.11 \
-P it-cassandra-4.0 \
-P !it-dse-6.8