## Building

To build locally run the following:

```sh
./mvnw clean package
```

* If you get a `Could not find or load main class org.apache.maven.wrapper.MavenWrapperMain` exception, upgrade your local `wget`
## Running Locally

### Prerequisite

Before starting Stargate locally, you will need an instance of Apache Cassandra&reg;.
The easiest way to do this is with a docker image (see [Cassandra docker images](https://hub.docker.com/_/cassandra)).

*NOTE* due to the way networking works with Docker for Mac, the Docker method only works on Linux. Use CCM (see below) for
use with Macs.

Docker: Start a Cassandra 3.11 instance

```sh
docker run --name local-cassandra \
--net=host \
-e CASSANDRA_CLUSTER_NAME=stargate \
-d cassandra:3.11.6
```

Cassandra Cluster Manager: Start a Cassandra 3.11 instance ([link to ccm](https://github.com/riptano/ccm))

```sh
ccm create -v 3.11.6 -b -n 1:0 -i 127.0.0.1 stargate && ccm start
```

### Start commands

Before starting on MacOSX you'll need to add an additional loopback

```sh
sudo ifconfig lo0 alias 127.0.0.2
```

Start Stargate from the command line with

```
./starctl --cluster-name stargate --cluster-seed 127.0.0.1 --cluster-version 3.11 --listen 127.0.0.2 --simple-snitch

# See all cli options with -h
```

Or use a pre-built docker image
Docker Hub: https://hub.docker.com/r/stargateio/

```sh
docker pull stargateio/stargate-3_11:v0.0.2
```

```sh
docker run --name stargate -d stargateio/stargate-3_11:v0.0.2 --cluster-name stargate --cluster-seed 127.0.0.1 --cluster-version 3.11 --listen 127.0.0.2 --simple-snitch
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
# Generate an auth token
curl -L -X POST 'http://localhost:8081/v1/auth' \
-H 'Content-Type: application/json' \
--data-raw '{
    "username": "username",
    "password": "password"
}'


# Get all keyspaces using the auth token from the previous request
curl -L -X GET 'localhost:8082/v1/keyspaces' \
--header 'accept: application/json' \
--header 'content-type: application/json' \
--header 'X-Cassandra-Token: <AUTH_TOKEN>'
```
