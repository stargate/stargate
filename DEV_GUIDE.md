## Code conventions

We use [google-java-format](https://github.com/google/google-java-format) for Java code, and
[xml-format-maven-plugin](https://github.com/acegi/xml-format-maven-plugin) for XML.

Both are integrated with Maven: the build will fail if some files are not formatted correctly.

To fix formatting issues from the command line, run the following:

```
mvn xml-format:xml-format fmt:format
```

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
ccm create stargate -v 3.11.6 -n 1 -s -b
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

## Running Integration Tests

Integration tests require that Cassandra Cluster Manager ([ccm](https://github.com/riptano/ccm))
be installed and accessible via the OS `PATH`.

Note: Integration tests use `ccm` to start transient storage nodes that are normally destroyed at 
the end of the test run. However, if the test JVM is killed during execution, the external storage
node may continue running and may interfere with subsequent test executions. In this case, the
transient storage process needs to be stopped manually (e.g. by using the `kill` command).

### Ordinary Execution

To run integration tests in the default configuration, run:

```shell
./mvnw verify
```

This will run integration tests for Cassandra 3.11 and 4.0. 
On a reasonably powerful laptop it takes about 40 minutes.

Note: Support for DSE is not turned on by default.
To build and test Stargate with the DSE 6.8 persistence module, run:

```shell
./mvnw verify -P dse -P it-dse-6.8
```

To run integration tests with all Cassandra and DSE persistence modules, run:

```shell
./mvnw verify -P it-cassandra-3.11 -P it-cassandra-4.0 -P dse -P it-dse-6.8
```

Note: Enabling only one of the `it-*` profiles will automatically disable the others.

### Debugging Integration Tests

When debugging integration tests, it may be preferable to manually control the storage node.
It does not matter how exactly the storage node is started (docker, ccm or manual run) as
long as port `7000` is properly forwarded from `127.0.0.1` to the storage node. If the storage
is managed manually, use the following options to convey connection information to the test JVM:
* `-Dstargate.test.backend.use.external=true`
* `-Dstargate.test.backend.cluster_name=<CLUSTER_NAME>`
* `-Dstargate.test.backend.dc=<DATA_CENTER_NAME>`
* `-Dstargate.test.backend.nodes=<NUMBER_OR_STORAGE_NODES>`

### Running / Debugging Integration Tests in an IDE

Integration tests can be started / debugged individually in an IDE.

If `ccm` is used to mange storage nodes during tests, it should be accessible from the IDE's
execution environment (`PATH`).
