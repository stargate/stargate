# Stargate REST API (Stargate V2)

This project provides the Stargate REST API - an HTTP service that allows access to data stored in a Cassandra cluster using a RESTful interface. (As part of Stargate V2, this service was extracted from the monolithic Stargate V1 Coordinator as a separate microservice.)

The project depends on the [sgv2-quarkus-common](../sgv2-quarkus-common) module, which defines common capabilities for all Stargate V2 APIs.

All issues related to this project are marked with the `stargate-v2` and `REST` [labels](https://github.com/stargate/stargate/issues?q=is%3Aopen+is%3Aissue+label%3Astargate-v2+label%3AREST).

REST API runs as a [Quarkus](https://quarkus.io/) service, different from Stargate V1 APIs which were built on the DropWizard framework.

##### Table of Contents
* [Concepts](#concepts)
    * [Shared concepts](#shared-concepts)
* [Deployment](#deployment)
    * [Running the service](#running-the-service)
    * [Configuration properties](#configuration-properties)
* [Development guide](#development-guide)
    * [Running the application in dev mode](#running-the-application-in-dev-mode)
    * [Running integration tests](#running-integration-tests)
    * [Packaging and running the application](#packaging-and-running-the-application)
    * [Creating a native executable](#creating-a-native-executable)
    * [Creating a docker image](#creating-a-docker-image)
* [Quarkus Extensions](#quarkus-extensions)

## Concepts

### Shared concepts

Please read the [Stargate V2 Shared Concepts](../sgv2-quarkus-common/README.md#shared-concepts) in order to get basic concepts shared between all V2 API implementations.

## Deployment

### Running the Service

The REST API service is packaged as a Docker image available on [Docker Hub](https://hub.docker.com/r/stargateio/restapi). For examples of container-based deployments, see the [docker-compose](../../docker-compose) or [helm](../../helm) folders. 

The REST API is also packaged as an uber-jar as part of Stargate v2 [releases](https://github.com/stargate/stargate/releases), along with the accompanying [start-restapi.sh](./start-restapi.sh) script. 

The startup script can also be used to start the uber-jar locally, along with other options documented in the [Development Guide](deployment-guide).

### Configuration properties

There are two main configuration property prefixes used, `stargate.` and `quarkus.`.

The `quarkus.` properties are defined by the Quarkus framework, and the complete list of available properties can be found on the [All configuration options](https://quarkus.io/guides/all-config) page.
In addition, the related guide of each [Quarkus extension](#quarkus-extensions) used in the project provides an overview of the available config options.

Shared `stargate.` properties are defined by the [sgv2-quarkus-common configuration](../sgv2-quarkus-common/CONFIGURATION.md).

REST API itself defines API-specific configuration properties: these are documented in the [Configuration Guide](CONFIGURATION.md).

Default configuration settings can be found from `application.yaml` files located in:

* `src/main/resources/application.yaml`: Production settings, used as the baseline for other modes
* `src/test/resources/application.yaml`: Overrides for "test" phase

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

> **Warning**
> This project uses Java 17, please ensure that you have the target JDK installed on your system.

### Running the application in dev mode

Before being able to run the application, make sure you install the root [../apis/pom.xml](../pom.xml):
```shell script
cd ../
./mvnw install -DskipTests
```

You can run your application in dev mode that enables live coding using:
```shell script
../mvnw quarkus:dev
```

By default, the application is served on port `8082`.

> **Note**  
> Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8082/stargate/dev/.

#### Debugging

In development mode, Quarkus starts by default with debug mode enabled, listening to port `5005` without suspending the JVM.
You can attach the debugger at any point, the simplest option would be from IntelliJ using `Run -> Attach to Process`.

If you wish to debug from the start of the application, start with `-Ddebug=client` and create a debug configuration in a *Listen to remote JVM* mode.

See [Debugging](https://quarkus.io/guides/maven-tooling#debugging) for more information.

### Running integration tests

> **Warning**  
> You need to build the coordinator docker image(s) first. In the Stargate repo directory `/apis` run:
> ```
> ../mvnw clean install -P dse -DskipTests && ./build_docker_images.sh
> ```

Integration tests are using the [Testcontainers](https://www.testcontainers.org/) library in order to set up all needed dependencies, a Stargate coordinator and a Cassandra data store.
They are separated from the unit tests and are running as part of the `integration-test` and `verify` Maven phases:
```shell script
../mvnw integration-test
```

#### Data store selection

Depending on the active profile, integration tests will target different Cassandra version as the data store.
The available profiles are:

* `cassandra-40` (enabled by default) - runs integration tests with [Cassandra 4.0](https://cassandra.apache.org/doc/4.0/index.html) as the data store
* `cassandra-311` - runs integration tests with [Cassandra 3.11](https://cassandra.apache.org/doc/3.11/index.html) as the data store
* `dse-68` - runs integration tests with [DataStax Enterprise (DSE) 6.8](https://docs.datastax.com/en/dse/6.8/dse-dev/index.html) as the data store

The required profile can be activated using the `-P` option:

```shell script
../mvnw integration-test -P cassandra-311
```

#### Running from IDE

> **Warning**  
> You need to build the coordinator docker image(s) first. In the Stargate repo directory `/apis` run:
> ```
> ../mvnw clean install -P dse -DskipTests && ./build_docker_images.sh
> ```

Running integration tests from an IDE is supported out of the box.
The tests will use the Cassandra 4.0 as the data store by default.
Running a test with a different version of the data store or the Stargate coordinator requires changing the run configuration and specifying the following system properties:

* `testing.containers.cassandra-image` - version of the Cassandra docker image to use, for example: `cassandra:4.0.4`
* `testing.containers.stargate-image` - version of the Stargate coordinator docker image to use, for example: `stargateio/coordinator-4_0:v2.0.1` (must be V2 coordinator for the target data store)
* `testing.containers.cluster-version` - version of the cluster, for example: `4.0` (should be one of `3.11`, `4.0` or `6.8`)
* `testing.containers.cluster-dse` - optional and only needed if DSE is used

#### Executing against a running application

The integration tests can also be executed against an already running instance of the application.
This can be achieved by setting the `quarkus.http.test-host` system property when running the tests.
You'll most likely need to specify the authentication token to use in the tests, by setting the `stargate.int-test.auth-token` system property.

```shell
./mvnw verify -DskipUnitTests -Dquarkus.http.test-host=1.2.3.4 -Dquarkus.http.test-port=4321 -Dstargate.int-test.auth-token=[AUTH_TOKEN]

```

#### Skipping integration tests

You can skip the integration tests during the maven build by disabling the `int-tests` profile using the `-DskipITs` property:

```shell script
../mvnw verify -DskipITs
```

#### Skipping unit tests

Alternatively you may want to run only integration tests but not unit tests (especially when changing integration tests).
This can be achieved using the command:

```
../mvnw verify -DskipUnitTests
```

#### Troubleshooting failure to run ITs

If your Integration Test run fails with some generic, non-descriptive error like:

```
[ERROR]   CollectionsResourceIntegrationTest » Runtime java.lang.reflect.InvocationTargetException
```

here are some things you should try:

* Make sure your Docker Engine has enough resources. For example following have been observed:
  * Docker Desktop defaults of 2 gigabytes of memory on Mac are not enough: try at least 4

### Packaging and running the application

The application can be packaged using:
```shell script
../mvnw package
```
This command produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
../mvnw package -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

### Creating a native executable

[Coming soon](https://github.com/stargate/stargate/issues/2155).

### Creating a Docker image

You can create a Docker image named `io.stargate/restapi` using:
```shell script
../mvnw clean package -Dquarkus.container-image.build=true
```

If you want to learn more about building container images, please consult [Container images](https://quarkus.io/guides/container-image).

## Quarkus Extensions

This project uses various Quarkus extensions, modules that run on top of a Quarkus application.
You can list, add and remove the extensions using the `quarkus ext` command.

> **Note**
> Please check the shared extensions introduced by the [sgv2-quarkus-common](../sgv2-quarkus-common/README.md#shared-quarkus-extensions) project.

### `quarkus-arc`
[Related guide](https://quarkus.io/guides/cdi-reference)

The Quarkus DI solution.

### `quarkus-container-image-docker`
[Related guide](https://quarkus.io/guides/container-image)

The project uses Docker for building the Container images.
Properties for Docker image building are defined in the [pom.xml](pom.xml) file.
Note that under the hood, the generated Dockerfiles under [src/main/docker](src/main/docker) are used when building the images.
When updating the Quarkus version, the Dockerfiles must be re-generated.

### `quarkus-smallrye-health`
[Related guide](https://quarkus.io/guides/smallrye-health)

The extension setups the health endpoints under `/stargate/health`.

### `quarkus-smallrye-openapi`
[Related guide](https://quarkus.io/guides/openapi-swaggerui)

The OpenAPI definitions are generated and available under `/api/rest/openapi` endpoint.
The [StargateDocsApi](src/main/java/io/stargate/sgv2/restapi/StargateRestApi.java) class defines the `@OpenAPIDefinition` annotation.
This definition defines the default *SecurityScheme* named `Token`, which expects the header based authentication with the HTTP Header `X-Cassandra-Token`.
The `info` portions of the Open API definitions are set using the `quarkus.smallrye-openapi.info-` configuration properties.
