#!/bin/bash
set -e

# Startup script for Stargate GraphQL API
#   This script can be used to start the GraphQL API with an uber-jar.
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
        echo "Stargate GraphQL API requires Java 17 or later."
        exit 1
    fi
fi

# Locate uber-jar file aka "runner"
GRAPHQLAPI_DIR=`dirname "$0"`
GRAPHQLAPI_JAR=$(find $GRAPHQLAPI_DIR -type f -iname "sgv2-graphqlapi-*-runner.jar")
if [ ! -e $GRAPHQLAPI_JAR ]; then
  echo "Unable to locate sgv2-graphqlapi runner.jar in $GRAPHQLAPI_DIR and subdirectories"
  exit 3
fi

java "$@" -jar $GRAPHQLAPI_JAR





