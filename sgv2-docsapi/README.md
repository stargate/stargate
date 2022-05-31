# Stargate Docs API V2

> **IMPORTANT:**  This project is currently an independent project in this monorepo, and it's not connected to the root `io.stargate:stargate` project.

This project represents the stand-alone Docs API microservice for Stargate V2, extracted from monolithic Stargate V1 Coordinator.

The high-level design of the application is described in the [Document API in Stargate V2 discussion](https://github.com/stargate/stargate/discussions/1742).
All issues related to the Docs API V2 are marked with the `stargate-v2` and `documents API` [labels](https://github.com/stargate/stargate/issues?q=is%3Aopen+is%3Aissue+label%3Astargate-v2+label%3A%22documents+API%22).

##### Table of Contents
* [Concepts](#concepts)
   * [Authentication](#authentication) 
   * [Multi-tenancy](#multi-tenancy) 
* [Configuration properties](#configuration-properties)  
* [Development guide](#development-guide)  
   * [Running the application in dev mode](#running-the-application-in-dev-mode)
   * [Running integration tests](#running-integration-tests)
   * [Packaging and running the application](#packaging-and-running-the-application) 
   * [Creating a native executable](#creating-a-native-executable) 
   * [Creating a docker image](#creating-a-docker-image) 
* [Quarkus Extensions](#quarkus-extensions)  

## Concepts

### Authentication

By default, the application uses the header-based authentication mechanism.
All requests must include the `X-Cassandra-Token` in order to be considered as authenticated.
The actual authentication and authorization is delegated to the Stargate V2 Bridge, by passing the value of that header as the Cassandra token.

The header-based authentication can be switched off and any [Quarkus Build-in Authentication Mechanism](https://quarkus.io/guides/security-built-in-authentication) can be used.
In that case,you need to provide a correct [CassandraTokenResolver](src/main/java/io/stargate/sgv2/docsapi/api/common/token/CassandraTokenResolver.java) implementation, in order to resolve the C* token when calling the Stargate V2 Bridge.

The [Configuration properties](#configuration-properties) provides detailed view on available configuration options.

### Multi-tenancy

The Stargate V2 explicitly supports the multi-tenancy.
Multi-tenancy is optional and is controlled by the configured [TenantResolver](src/main/java/io/stargate/sgv2/docsapi/api/common/tenant/TenantResolver.java) implementation.
If a tenant is resolved for an HTTP request, the information about the tenant will be propagated to the Stargate V2 Bridge.
In addition, the tenant identifier could be added to metrics, traces, logs, etc.

The [Configuration properties](#configuration-properties) provides detailed view on available configuration options.

## Configuration properties

There are two main configuration property prefixes used, `stargate.` and `quarkus.`.

The `quarkus.` properties are defined by the Quarkus framework, and the complete list of available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.
In addition, the related guide of each [Quarkus extension](#quarkus-extensions) used in the project, gives overview on the available config options as well.

The `stargate.` properties are defined by this project itself.
The properties are defined by dedicated config classes annotated with the `@ConfigMapping`.
The list of currently available properties is documented in the [Configuration Guide](CONFIGURATION.md).

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

### Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8180/stargate/dev/.

#### Debugging

In development mode, Quarkus starts by default with debug mode enabled, listening to port `5005` without suspending the JVM.
You can attach the debugger at any point, the simplest option would be from IntelliJ using `Run -> Attach to Process`.

If you wish to debug from the start of the application, start with `-Ddebug=client` and create a debug configuration in a *Listen to remote JVM* mode.

See [Debugging](https://quarkus.io/guides/maven-tooling#debugging) for more information.

### Running integration tests

> **_PREREQUISITES:_**  You need to build the coordinator docker image(s) first. In the root Stargate repo directory run:
> ```
> ./mvnw clean install -P dse -DskipTests && ./build_docker_images.sh
> ```

Integration tests are using the [Testcontainers](https://www.testcontainers.org/) library in order to set up all needed dependencies, a Stargate coordinator and a Cassandra data store. 
They are separated from the unit tests and are running as part of the `integration-test` and `verify` Maven phases:
```shell script
./mvnw integration-test
```

#### Data store selection

Depending on the active profile, integration tests will target different Cassandra version as the data store.
The available profiles are:

* `cassandra-40` (enabled by default) - runs integration tests with [Cassandra 4.0](https://cassandra.apache.org/doc/4.0/index.html) as the data store
* `cassandra-311` - runs integration tests with [Cassandra 3.11](https://cassandra.apache.org/doc/3.11/index.html) as the data store
* `dse-68` - runs integration tests with [DataStax Enterprise (DSE) 6.8](https://docs.datastax.com/en/dse/6.8/dse-dev/index.html) as the data store

The required profile can be activated using the `-P` option:

```shell script
./mvnw integration-test -P cassandra-311
```

#### Running from IDE

> **_PREREQUISITES:_**  You need to build the coordinator docker image(s) first and tag them with `latest` tag. In the root Stargate repo directory run:
> ```
> ./mvnw clean install -P dse -DskipTests && ./build_docker_images.sh -t latest
> ```

Running integration tests from IDE is supported out of the box.
The tests will use the Cassandra 4.0 as the data store by default.
Running a test with a different version of the data store or the Stargate coordinator, requires changing the run configuration and specifying the following system properties:

* `testing.containers.cassandra-image` - version of the Cassandra docker image to use, for example: `cassandra:4.0.3`
* `testing.containers.stargate-image` - version of the Stargate coordinator docker image to use, for example: `stargateio/coordinator-4_0:v2.0.0-ALPHA-10-SNAPSHOT` (must be V2 coordinator for the target data store)
* `testing.containers.cluster-version` - version of the cluster, for example: `4.0` (should be one of `3.11`, `4.0` or `6.8`)
* `testing.containers.cluster-dse` - optional and only needed if DSE is used

#### Skipping integration tests

You can skip the integration tests during the maven build by disabling the `int-tests` profile using the `-DskipIntTests` property:

```shell script
./mvnw verify -DskipIntTests
```

### Packaging and running the application

The application can be packaged using:
```shell script
./mvnw package
```
It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
./mvnw package -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

### Creating a native executable

You can create a native executable using: 
```shell script
./mvnw package -Pnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: 
```shell script
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/sgv2-docsapi-${project.version}-runner`

If you want to learn more about building native executables, please consult [Maven tooling](https://quarkus.io/guides/maven-tooling).

### Creating a docker image

You can create a docker image named `io.stargate/docsapi` using:
```shell script
./mvnw clean package -Dquarkus.container-image.build=true
```

Or, if you want to create a native-runnable docker image named `io.stargate/docsapi-native` using:
```shell script
./mvnw clean package -Pnative -Dquarkus.native.container-build=true -Dquarkus.container-image.build=true
```

If you want to learn more about building container images, please consult [Container images](https://quarkus.io/guides/container-image).

## Quarkus Extensions

This project uses various Quarkus extensions, modules that run on top of a Quarkus application.
You can list, add and remove the extensions using the `quarkus ext` command.

### `quarkus-config-yaml`
[Related guide](https://quarkus.io/guides/config-yaml)

The application configuration is specified using the YAML format.
Default configuration properties are specified in the [application.yaml](src/main/resources/application.yaml) file.
The YAML configuration also support profile aware files. 
In this case, properties for a specific profile may reside in an `application-{profile}.yaml` named file.

### `quarkus-container-image-docker`
[Related guide](https://quarkus.io/guides/container-image)

The project uses Docker for building the Container images.
Properties for the docker image building are defined in the [pom.xml](pom.xml).
Note that under the hood, the generated Dockerfiles under [src/main/docker](src/main/docker) are used when building the images.
When updating the Quarkus version, the Dockerfiles must be re-generated.

### `quarkus-grpc`
[Related guide](https://quarkus.io/guides/grpc)

The gRPC extension provides an easy way for consuming gRPC services, in this case the Stargate Bridge gRPC service.
The [pom.xml](pom.xml) file defines what proto files should be used for generating the stubs:

```xml
<quarkus.generate-code.grpc.scan-for-proto>io.stargate.grpc:grpc-proto</quarkus.generate-code.grpc.scan-for-proto>
```

The clients are configured using the `quarkus.grpc.clients.[client-name]` properties.
In order to pass the request context information to the Bridge, the client should be obtained using the [GrpcClients](src/main/java/io/stargate/sgv2/docsapi/grpc/GrpcClients.java) utility class.

### `quarkus-hibernate-validator`
[Related guide](https://quarkus.io/guides/validation)

Enables validation of configuration, beans, REST endpoints, etc.

### `quarkus-micrometer-registry-prometheus`
[Related guide](https://quarkus.io/guides/micrometer)

Enables out-of-the-box metrics collection and the Prometheus exporter.
Metrics are exported at the Prometheus default `/metrics` endpoint.
All non-application endpoints are ignored from the collection.

### `quarkus-opentelemetry-exporter-otlp`
[Related guide](https://quarkus.io/guides/opentelemetry)

Enables the [OpenTelemetry](https://opentelemetry.io/) tracing support.
In order to activate the tracing, you need to supply the OTLP gRPC endpoint using the JVM parameters `-Dquarkus.opentelemetry.tracer.exporter.otlp.enabled=true -Dquarkus.opentelemetry.tracer.exporter.otlp.endpoint=http://localhost:55680`.
The easiest way to locally collect and visualize traces is to use the [jaegertracing/opentelemetry-all-in-one](https://www.jaegertracing.io/docs/1.21/opentelemetry/) docker image with [in-memory](https://www.jaegertracing.io/docs/1.21/deployment/#badger---local-storage) storage:

```shell
docker run \
  --rm \
  -e SPAN_STORAGE_TYPE=badger \
  -e BADGER_EPHEMERAL=false \
  -e BADGER_DIRECTORY_VALUE=/badger/data \
  -e BADGER_DIRECTORY_KEY=/badger/key \
  -v badger:/badger \
  -p 55680:55680 \
  -p 16686:16686 \
  jaegertracing/opentelemetry-all-in-one
```

You can then visualize traces using Jaeger UI started on http://localhost:16686.

### `quarkus-resteasy-reactive`
[Related guide](https://quarkus.io/guides/resteasy-reactive)

The project uses the reactive implementation of the `resteasy` to create HTTP resources.
The JSON serialization is done using the Jackson library (provided by `quarkus-resteasy-reactive-jackson` extension).
The exception handling is done using the dedicated handlers annotated with `org.jboss.resteasy.reactive.server.ServerExceptionMapper`.

By default, the application is served on port `8180`.
The root path for the non-application endpoints (health, Open API, etc.) is `/stargate`.
All configuration options for the HTTP server can be found in https://quarkus.io/guides/all-config#quarkus-vertx-http_quarkus-vertx-http-eclipse-vert.x-http.

### `quarkus-resteasy-reactive`
[Related guide](https://quarkus.io/guides/resteasy-reactive)

The project uses the reactive implementation of the `resteasy` to create HTTP resources.
The exception handling is done using the dedicated handlers annotated with `org.jboss.resteasy.reactive.server.ServerExceptionMapper`.

By default, the application is served on port `8180`.
The root path for the non-application endpoints (health, Open API, etc.) is `/stargate`.
All configuration options for the HTTP server can be found in https://quarkus.io/guides/all-config#quarkus-vertx-http_quarkus-vertx-http-eclipse-vert.x-http.

### `quarkus-smallrye-health`
[Related guide](https://quarkus.io/guides/smallrye-health)

The extension setups the health endpoints under `/stargate/health`.

### `quarkus-smallrye-openapi`
[Related guide](https://quarkus.io/guides/openapi-swaggerui)

The OpenAPI definitions are generated and available under `/stargate/openapi` endpoint.
The [StargateDocsApi](src/main/java/io/stargate/sgv2/docsapi/StargateDocsApi.java) class defines the `@OpenAPIDefinition` annotation.
This definition defines the default *SecurityScheme* named `Token`, which expects the header based authentication with the HTTP Header `X-Cassandra-Token`.
The `info` part of the Open API definitions are set using the `quarkus.smallrye-openapi.info-` configuration properties.

The Swagger UI is available under `/swagger-ui` endpoint.
Note that Swagger UI is by default enabled in production profile and can be disabled by setting the `quarkus.swagger-ui.always-include` property to `false`.
