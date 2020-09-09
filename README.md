# Stargate

![Java CI](https://github.com/stargate/stargate/workflows/Java%20CI/badge.svg)

## Introduction

An open framework that makes it easy to use Apache Cassandra&reg; for any workload by adding plug-in support for new APIs, data 
types, and data access methods

## Building

To build locally run the following:

```sh
./mvnw clean package
```

## Running Locally

### Prerequisite

Before starting locally you will need an instance of either Apache Cassandra&reg;. The easiest way to do this is with a 
docker image. 

*NOTE* due to the way networking works with Docker for Mac, the Docker method only works on Linux. Use CCM (see below) for 
use with Macs.

Apache Cassandra&reg;

```sh
docker run --name local-cassandra \
--net=host \
-e CASSANDRA_CLUSTER_NAME=stargate \
-d cassandra:3.11.6
```

or to run locally with [ccm](https://github.com/riptano/ccm)

```sh
ccm create -v 3.11.6 -b -n 1:0 -i 127.0.0 stargate && ccm start
```

### Start commands

Before starting on OSX you'll need to add an additional loopback

```sh
sudo ifconfig lo0 alias 127.0.0.2
```

Can run from the command line with

```
./starctl --cluster-name stargate --cluster-seed 127.0.0.1 --cluster-version 3.11 --listen 127.0.0.2 --simple-snitch

# See all cli options with -h
```

Or with Docker

```sh
docker build -t stargate . && docker run -i -t stargate
```

`starctl` respects the `JAVA_OPTS` environment variable. 
For example, to set a java system property with spaces in its value one can run `starctl` as follows. 
Note the double quotes embedded in the env. var value - it is re-evalutated (once) as a `bash` token before being
passed to the JVM. This is required to break the single value of `JAVA_OPTS` into a sequence of tokens.
This kind of processing is not required for ordinary command line arguments, therefore they do not need any extra
quoting.

```shell script
env JAVA_OPTS='-Dmy_property="some value"' ./starctl --cluster-name 'Some Cluster' ...
```

### Debugging

If you're an IntelliJ user you can use start the project with

```sh
java -jar -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 -Dstargate.libdir=./stargate-lib stargate-lib/stargate-starter-1.0-SNAPSHOT.jar
```

Alternatively, use the `JAVA_OPTS` environment variable to pass debugging options to the JVM

```shell script
env JAVA_OPTS='-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005' ./starctl --cluster-name stargate ...
```

and then follow the steps found [here](https://www.baeldung.com/intellij-remote-debugging)


## Connecting

### CQL

Connect to CQL as normal on port 9042

```sh
$ cqlsh 127.0.0.2 9042
Connected to stargate at 127.0.0.2:9042.
[cqlsh 5.0.1 | Cassandra 3.11.6 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
```

### REST

Curl over port 8082

```sh
curl -L -X GET 'localhost:8082/v1/keyspaces' \
--header 'accept: application/json' \
--header 'content-type: application/json' \
--header 'X-Cassandra-Token: <AUTH_TOKEN>'
```
