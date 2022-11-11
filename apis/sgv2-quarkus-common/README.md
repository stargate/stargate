# Stargate V2 Quarkus Common

This project holds shared functionality, configuration and concepts for all Stargate V2 APIs.

##### Table of Contents
* [Shared Concepts](#shared-concepts)
   * [Authentication](#authentication)
   * [Consistency](#consistency-levels) 
   * [Metrics](#metrics)
   * [Multi-tenancy](#multi-tenancy) 
* [Configuration properties](#configuration-properties)  
* [Development guide](#development-guide)  
   * [Running tests](#running-tests)

## Shared Concepts

### Authentication

By default, the APIs use the header-based authentication mechanism.
All requests must include the `X-Cassandra-Token`.
The actual authentication and authorization checks are delegated to the Stargate V2 Bridge, by passing the value of that header as the Cassandra token.

The header-based authentication can be switched off and any [Quarkus Build-in Authentication Mechanism](https://quarkus.io/guides/security-built-in-authentication) can be used.
In that case, you need to provide a correct [CassandraTokenResolver](src/main/java/io/stargate/sgv2/api/common/token/CassandraTokenResolver.java) implementation, in order to resolve the Cassandra token when calling the Stargate V2 Bridge.

The [Authentication & security configuration](CONFIGURATION.md#authentication--security-configuration) provides a detailed description of the available configuration options.

### Consistency levels

The common project enables configuration of the Cassandra consistency levels used in Stargate APIs.
The consistency levels can be defined separately for queries doing:

1. schema changes
2. writes
3. reads

The [Queries configuration](CONFIGURATION.md#queries-configuration) provides a detailed view of the available configuration options.

### Metrics

All Stargate V2 APIs export Prometheus-based metrics.
The metrics collection is performed using the `quarkus-micrometer-registry-prometheus` extension. The common module enables additional metric configuration.

The [Metrics configuration](CONFIGURATION.md#metrics-configuration) provides a detailed view of the available configuration options.

### Multi-tenancy

The Stargate V2 provides support for multi-tenancy.
Multi-tenancy is optional and is controlled by the configured [TenantResolver](src/main/java/io/stargate/sgv2/api/common/tenant/TenantResolver.java) implementation.
If a tenant is resolved for an HTTP request, the information about the tenant will be propagated to the Stargate V2 Bridge.
In addition, the tenant identifier can be used to tag metrics, or included in logs or traces.

The [Multi-tenancy configuration](CONFIGURATION.md#multi-tenancy-configuration) provides a detailed view of the available configuration options.

## Configuration properties

This project defines several common `stargate.` properties using dedicated config classes annotated with the `@ConfigMapping`.
The list of currently available properties is documented in the [Configuration Guide](CONFIGURATION.md).

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

We recommended that you install the Quarkus CLI for the best development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

> **_WARNING:_**  The common module is not made as a running application, but rather as a dependency used in the other modules.

### Running tests

The tests in this project can be run using the command:

```shell script
../mvnw test
```

To build without executing the tests, run:

```shell script
../mvnw install -DskipUnitTests
```

> **_NOTE:_**  The common module does not contain integration tests.

## Shared Quarkus extensions

This project depends on various Quarkus extensions, modules that run on top of a Quarkus application.
These extensions are shared between all API projects.

### `quarkus-config-yaml`
[Related guide](https://quarkus.io/guides/config-yaml)

The application configuration is specified using the YAML format.
Default configuration properties are specified in the [application.yaml](src/main/resources/application.yaml) file.
The YAML configuration also support profile aware files.
In this case, properties for a specific profile may reside in an `application-{profile}.yaml` named file.

### `quarkus-grpc`
[Related guide](https://quarkus.io/guides/grpc)

The gRPC extension provides an easy way for consuming gRPC services, in this case the Stargate Bridge gRPC service.
The [pom.xml](pom.xml) file defines what proto files should be used for generating the stubs:

```xml
<quarkus.generate-code.grpc.scan-for-proto>io.stargate.grpc:grpc-proto</quarkus.generate-code.grpc.scan-for-proto>
```

The clients are configured using the `quarkus.grpc.clients.[client-name]` properties.
In order to pass the request context information to the Bridge, the client should be obtained using the [GrpcClients](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/grpc/GrpcClients.java) utility class.

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
The easiest way to locally collect and visualize traces is to use the [jaegertracing/opentelemetry-all-in-one](https://www.jaegertracing.io/docs/1.21/opentelemetry/) Docker image with [in-memory](https://www.jaegertracing.io/docs/1.21/deployment/#badger---local-storage) storage:

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

By default, the application is served based on the `quarkus.http.port` configuration value and varies between API projects.
The root path for the non-application endpoints (health, Open API, etc.) is `/stargate`.
All configuration options for the HTTP server can be found in [Quarkus Configuration Options](https://quarkus.io/guides/all-config#quarkus-vertx-http_quarkus-vertx-http-eclipse-vert.x-http).

### `quarkus-smallrye-openapi`
[Related guide](https://quarkus.io/guides/openapi-swaggerui)

The OpenAPI definitions are generated and available under `/api/docs/openapi` endpoint.
The `info` portions of the Open API definitions are set using the `quarkus.smallrye-openapi.info-` configuration properties.

The Swagger UI is available at the `/swagger-ui` endpoint.
Note that Swagger UI is enabled by default in the production profile and can be disabled by setting the `quarkus.swagger-ui.always-include` property to `false`.
