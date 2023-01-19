#!/bin/bash
set -e

# Startup script for Stargate REST API
#   This script can be used to start the REST API with an uber-jar.
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
        echo "Stargate REST API requires Java 17 or later."
        exit 1
    fi
fi

# Locate uber-jar file aka "runner"
RESTAPI_DIR=`dirname "$0"`
RESTAPI_JAR=$(find $RESTAPI_DIR -type f -iname "sgv2-restapi-*-runner.jar")
if [ ! -e $RESTAPI_JAR ]; then
  echo "Unable to locate sgv2-restapi runner.jar in $RESTAPI_DIR and subdirectories"
  exit 3
fi

java -jar $RESTAPI_JAR "$@"





