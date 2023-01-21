#!/bin/bash
set -e

# Startup script for Stargate Document API
#   This script can be used to start the Document API with an uber-jar.
#   Any Java properties/options provided as arguments to this script are passed along to the `java` command.
#   See the README.md for this project for instructions on building the uber-jar and available Java properties.

# Enforce minimum Java version
if type -p java >/dev/null; then
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    _java="$JAVA_HOME/bin/java"
else
    echo "\$JAVA_HOME not found"
    exit 1
fi

if [[ "$_java" ]]; then
    jvmver=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    JVM_VERSION=${jvmver%_*}

    if [ "$JVM_VERSION" \< "17" ] ; then
        echo "Stargate Docs API requires Java 17 or later."
        exit 1
    fi
fi

# Locate uber-jar file aka "runner"
DOCSAPI_DIR=`dirname "$0"`
DOCSAPI_JAR=$(find $DOCSAPI_DIR -type f -iname "sgv2-docsapi-*-runner.jar")
if [ ! -e $DOCSAPI_JAR ]; then
  echo "Unable to locate sgv2-docsapi runner.jar in $DOCSAPI_DIR and subdirectories"
  exit 3
fi

# Settings for gRPC bridge can be overridden via Java properties passed via command line
#   -Dquarkus.grpc.clients.bridge.host=myhost
#   -Dquarkus.grpc.clients.bridge.host=myport
export QUARKUS_GRPC_CLIENTS_BRIDGE_HOST=localhost
export QUARKUS_GRPC_CLIENTS_BRIDGE_PORT=8091

java "$@" -jar $DOCSAPI_JAR
