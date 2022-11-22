## Stargate Developer Guide

Want to build Stargate locally or even start contributing to the project? This is the right place to get started. 

If you're developing on MacOS, we've added notes throughout to highlight a few specific differences.
 
## Code conventions

We use [google-java-format](https://github.com/google/google-java-format) for Java code, and
[xml-format-maven-plugin](https://github.com/acegi/xml-format-maven-plugin) for XML.

Both are integrated with Maven: the build will fail if some files are not formatted correctly.

To fix formatting issues from the command line, run the following:

```sh
mvn xml-format:xml-format fmt:format
```

## Java Version 

Stargate currently runs on Java 8 due to its backend dependencies. It's important to ensure that you have the correct JDK 8 installed before you can successfully compile the Stargate project. There are a number of versions of JDK 8 and a number of different ways to install them, but not all of them will work successfully with Stargate.

Download JDK 8 from this link: https://adoptopenjdk.net/?variant=openjdk8&jvmVariant=hotspot

Install the JDK and add it to your path. 

For example: if you are using a newer version of MacOS, then you are likely using Z-Shell (zsh) by default. So open your `~/.zshrc` file and add the path there:

```sh
export JAVA_HOME="/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

## Building with Maven

Stargate uses Maven for builds. You can download and install Maven from this [link](https://maven.apache.org/download.cgi) or use the included [maven wrapper](https://github.com/stargate/stargate/blob/main/mvnw) script as you would ordinarily use the `mvn` command.

To build locally run the following:

```sh
./mvnw clean package
```

You can also build a single module like this:

```sh
./mvnw package -pl restapi -am
```

> * **_NOTE:_** If you get a `Could not find or load main class org.apache.maven.wrapper.MavenWrapperMain` 
> exception on Linux, upgrade your local `wget`.

## Running Locally

### Prerequisite

Before starting Stargate locally, you will need an instance of Apache Cassandra&reg;.
The easiest way to do this is with a Docker image (see [Cassandra docker images](https://hub.docker.com/_/cassandra)).

> **_NOTE:_** due to the way networking works with Docker for Mac, the Docker method only works on Linux. 
> We recommend CCM (see below) for use with MacOS.

Docker: Start a Cassandra 3.11 instance:

```sh
docker run --name local-cassandra \
--net=host \
-e CASSANDRA_CLUSTER_NAME=stargate \
-d cassandra:3.11.13
```

Cassandra Cluster Manager: Start a Cassandra 3.11 instance ([link to ccm](https://github.com/riptano/ccm))

```sh
ccm create stargate -v 3.11.13 -n 1 -s -b
```

### Start commands

> **_NOTE:_**  Before starting Stargate on MacOS you'll need to add an additional loopback:

```sh
sudo ifconfig lo0 alias 127.0.0.2
```

Start Stargate from the command line as follows:

```sh
./starctl --cluster-name stargate --cluster-seed 127.0.0.1 --cluster-version 3.11 --listen 127.0.0.2 --bind-to-listen-address --simple-snitch

# See all cli options with -h
```

Or use a pre-built image from [Docker Hub](https://hub.docker.com/r/stargateio/):

```sh
docker pull stargateio/stargate-3_11:v0.0.2

docker run --name stargate -d stargateio/stargate-3_11:v0.0.2 --cluster-name stargate --cluster-seed 127.0.0.1 --cluster-version 3.11 --listen 127.0.0.2 --simple-snitch
```

The `starctl` script respects the `JAVA_OPTS` environment variable.
For example, to set a Java system property with spaces in its value, run `starctl` as shown below.
Note the double quotes embedded in the environment var value - it is re-evalutated (once) as a `bash` 
token before being passed to the JVM. This is required to break the single value of `JAVA_OPTS` into 
a sequence of tokens. This kind of processing is not required for ordinary command line arguments, 
therefore they do not need any extra quoting.

```sh script
env JAVA_OPTS='-Dmy_property="some value"' ./starctl --cluster-name 'Some Cluster' ...
```

### Debugging

If you're an IntelliJ user you can create the *JAR Application* run configuration, pointing to the `stargate-lib/stargate-starter-[VERSION].jar` and specifying `stargate-lib/` as the working directory.

Then disable **Instrumenting agent** in `Settings | Build, Execution, Deployment | Debugger | Async Stacktraces`.
This will allow you to debug directly using the IntelliJ debug run option.
You can debug any run configuration and tests as well.

#### Remote debugging

```sh
java -jar -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 -Dstargate.libdir=./stargate-lib stargate-lib/stargate-starter-1.0-SNAPSHOT.jar
```

Alternatively, use the `JAVA_OPTS` environment variable to pass debugging options to the JVM

```sh script
env JAVA_OPTS='-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005' ./starctl --cluster-name stargate ...
```

Then follow the steps found [here](https://www.baeldung.com/intellij-remote-debugging).

## Connecting

### CQL

Connect to CQL as normal on port 9042:

```sh
$ cqlsh 127.0.0.2 9042
Connected to stargate at 127.0.0.2:9042.
[cqlsh 5.0.1 | Cassandra 3.11.13 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
```

### REST

First, get an auth token to use on subsequent requests: 

```sh
# Generate an auth token
curl -L -X POST 'http://127.0.0.2:8081/v1/auth' \
-H 'Content-Type: application/json' \
--data-raw '{
    "username": "cassandra",
    "password": "cassandra"
}'
```

Then use the token when accessing the REST API:

```sh
# Get all keyspaces using the auth token from the previous request
curl -L -X GET '127.0.0.2:8082/v1/keyspaces' \
--header 'accept: application/json' \
--header 'content-type: application/json' \
--header 'X-Cassandra-Token: <AUTH_TOKEN>'
```

## Running Integration Tests

Integration tests require that Cassandra Cluster Manager ([ccm](https://github.com/riptano/ccm))
be installed and accessible via the OS `PATH`.

The tests use `ccm` to start transient storage nodes that are normally destroyed at 
the end of the test run. However, if the test JVM is killed during execution, the external storage
node may continue running and may interfere with subsequent test executions. In this case, the
transient storage process needs to be stopped manually (e.g. by using the `kill` command).

> **_NOTE:_** to run integration tests on MacOS, you'll need to enable several different loopback addresses 
> using the instructions [below](#loopback-addresses).

### Ordinary Execution

To run integration tests in the default configuration, run:

```sh
./mvnw verify
```

This will run integration tests for Cassandra 3.11 and 4.0. 
On a reasonably powerful laptop it takes about 40 minutes.

Note: Support for DSE is disabled by default.
To build and test Stargate with the DSE 6.8 persistence module, specify the `dse` and `it-dse-6.8` Maven profiles:

```sh
./mvnw verify -P dse -P it-dse-6.8
```

To run integration tests with all Cassandra and DSE persistence modules, run:

```sh
./mvnw verify -P it-cassandra-3.11 -P it-cassandra-4.0 -P dse -P it-dse-6.8
```

Note: Enabling only one of the `it-*` Maven profiles will automatically disable the others.

### Running a Single Integration Test
If you're working with a single test to get something working or adding a new test, you may want to run with
just that one test rather than waiting for the entire IT suite to complete. To do this, first make sure you 
have done a recent build, for example:

```sh
./mvnw clean install -DskipTests
```

Then you can run the individual test using the `-Dit.test` option, for example:

```sh
mvn -pl testing -Pit-cassandra-3.11 verify -Dit.test=RestApiv2SchemaTest
```

You can even run a single case (method):

```sh
mvn -pl testing -Pit-cassandra-3.11 verify -Dit.test="RestApiv2SchemaTest#tableWithMixedCaseNames"
```

### Debugging Integration Tests

When debugging integration tests, you may prefer to manually control the storage node.
It does not matter how exactly the storage node is started (Docker, ccm or manual run) as
long as port `7000` is properly forwarded from `127.0.0.1` to the storage node. If you are managing 
the storage node manually, use the following options to convey connection information to the test JVM:
* `-Dstargate.test.backend.use.external=true`
* `-Dstargate.test.backend.cluster_name=<CLUSTER_NAME>`
* `-Dstargate.test.backend.dc=<DATA_CENTER_NAME>`

When integration tests run with debugging options, the related Stargate nodes will also be
started with debugging options (using consecutive ports starting with 5100), for example:

```sh
-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=localhost:5100
```

You can run multiple Java debuggers waiting for connections on ports `510N` and up -
one for each Stargate node required for the test. Note that most of the tests start only
one Stargate node.

The picture below shows the remote listening debug run configuration in IntelliJ.
That configuration must be started before running the integration test in the debug mode.
You wlll observe two or more JVMs in the debug model, one running the integration tests and at least one running Stargate. 

![image](assets/remote-debug-listener.png#center)

### Running / Debugging Integration Tests in an IDE

You can start and debug integration tests individually in an IDE.

If you're using `ccm` to manage storage nodes during tests, it must be accessible from the IDE's
execution environment (`PATH`).

### Specifying Storage Backend

When tests are started manually via an IDE or JUnit Console Launcher, you can specify the type and version
of the storage backend using the following Java system properties:

* `-Dccm.version=<version>` - the version of the storage cluster (e.g. `3.11.13`)
* `-Dccm.dse=<true|false>` - specifies whether the storage cluster is DSE or OSS Cassandra.
  If `false` this option can be omitted.

### Adding New Integration Tests

There are two custom JUnit 5 extensions used when running integration tests.

* `ExternalStorage` - manages starting and stopping storage nodes (Cassandra and/or DSE) through
  [ccm]((https://github.com/riptano/ccm)).
  This extension is defined in the `persistence-test` module.
  The `@ClusterSpec` annotation works in conjunction with `ExternalStorage` and defines parameters
  of the external storage nodes.
  When this extension is active, it will automatically inject test method parameters of type
  `ClusterConnectionInfo`.

* `StargateCoordinator` - manages starting and stopping Stargate nodes.
  This extension is defined in the `testing` module.
  The `@StargateSpec` annotation works in conjunction with `StargateCoordinator` and defines parameters
  of the Stargate nodes.
  When this extension is active, it will automatically inject test method parameters of type
  `StargateConnectionInfo` and `StargateEnvironmentInfo`.

Integration tests that do not need Stargate nodes (e.g. `Cassandra40PersistenceIT`) can use only
the `ExternalStorage` extension by having the `@ExtendWith(ExternalStorage.class)` annotation
either directly on the test class or on one of its super-classes.

Integration tests that need both storage and Stargate nodes, should use the `@UseStargateCoordinator`
annotation to activate both extensions in the right order.

The code element holding `@ClusterSpec` or `@StargateSpec` annotations controls the lifecycle of
the nodes they define. If the "spec" is present at the class level (inherited), the corresponding 
nodes will be started/stopped according to `@BeforeAll` / `@AfterAll` JUnit 5 callbacks. Similarly,
if the spec is present at the method level, each node's lifecycle will follow `@BeforeEach` /
`@AfterEach` callbacks. An exception to this rule is when the spec has the `shared` property set 
to `true`, in which case the corresponding nodes will not be stopped until another test is executed
and that test requests _different_ node parameters (when that happens the old nodes will be stopped,
and the new node(s) will be started before executing the new test). If the spec annotations are not
present on the code element, no action is taken by the extensions and storage / Stargate nodes 
may or may not be available to the test depending on what happened before in the test execution
context.

Parameter injection works with any method where JUnit 5 supports parameter injection
(e.g. constructors, `@Test` methods, `@Before*` methods) if the corresponding storage / Stargate
nodes are available.

### <a name="loopback-addresses"></a> Creating Loopback Addresses for Integration Tests on MacOS

The integration tests use multiple loopback addresses which you will need to create individually on MacOS. 
We recommend persisting the network aliases using a RunAtLoad launch daemon which OSX automatically loads 
on startup. For example:

Create a shell script:
```
sudo vim /Library/LaunchDaemons/com.ccm.lo0.alias.sh
```

Contents of the script:
```
#!/bin/sh

# create loopback addresses used by Stargate integration tests

# 127.0.0.2 - 127.0.0.11
for ((i=2;i<12;i++))
do
    sudo /sbin/ifconfig lo0 alias 127.0.0.$i;
done

# 127.0.1.11 - 127.0.1.12
for ((i=11;i<13;i++))
do
    sudo /sbin/ifconfig lo0 alias 127.0.1.$i;
done

# 127.0.2.1 - 127.0.2.60
for ((i=1;i<61;i++))
do
    sudo /sbin/ifconfig lo0 alias 127.0.2.$i;
done

```

Set access of the script:
```
sudo chmod 755 /Library/LaunchDaemons/com.ccm.lo0.alias.sh
```

Create a plist to launch the script:
```
sudo vim /Library/LaunchDaemons/com.ccm.lo0.alias.plist
```

Contents of the plist:
```
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ccm.lo0.alias</string>
    <key>RunAtLoad</key>
    <true/>
    <key>ProgramArguments</key>
    <array>
      <string>/Library/LaunchDaemons/com.ccm.lo0.alias.sh</string>
    </array>
    <key>StandardErrorPath</key>
    <string>/var/log/loopback-alias.log</string>
    <key>StandardOutPath</key>
    <string>/var/log/loopback-alias.log</string>
</dict>
</plist>
```

Set access of the plist:
```
sudo chmod 0644 /Library/LaunchDaemons/com.ccm.lo0.alias.plist
sudo chown root:staff /Library/LaunchDaemons/com.ccm.lo0.alias.plist
```

Launch the daemon now. MacOS will automatically reload it on startup.
```
sudo launchctl load /Library/LaunchDaemons/com.ccm.lo0.alias.plist
```

Verify you can ping 127.0.0.2 and 127.0.0.3, etc.

If you ever want to permanently kill the daemon, simply delete its plist from /Library/LaunchDaemons/.

## Updating Licenses Report

To update the `licenses-report.txt` you'll need to install [fossa-cli](https://github.com/fossas/fossa-cli). Once
you have that installed locally run the following from the root `stargate` directory.

```sh
FOSSA_API_KEY=<TOKEN> fossa
FOSSA_API_KEY=<TOKEN> fossa report licenses > foo.txt
```

It's best to write the report to a temporary file and use your diff
tool of choice to merge the two together since fossa-cli generates a ton of duplicates.

Finally, before committing your changes you'll want to clean up:

```sh
rm foo.txt .fossa.yml
```




