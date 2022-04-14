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
   * [Running the application in dev mode](#packaging-and-running-the-application) 
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
Below is the list of currently available properties.

#### [gRPC metadata configuration](src/main/java/io/stargate/sgv2/docsapi/config/GrpcMetadataConfig.java)
*Configuration for the gRPC metadata passed to the Bridge.*

| Property                                     | Type     | Default             | Description                                                 |
|----------------------------------------------|----------|---------------------|-------------------------------------------------------------|
| `stargate.grpc-metadata.tenant-id-key`       | `String` | `X-Tenant-Id`       | Metadata key for passing the tenant-id to the Bridge.       |
| `stargate.grpc-metadata.cassandra-token-key` | `String` | `X-Cassandra-Token` | Metadata key for passing the cassandra token to the Bridge. |

#### [Header-based authentication configuration](src/main/java/io/stargate/sgv2/docsapi/config/HeaderBasedAuthConfig.java)
*Configuration for the header based authentication.*

| Property                                 | Type      | Default             | Description                          |
|------------------------------------------|-----------|---------------------|--------------------------------------|
| `stargate.header-based-auth.enabled`     | `boolean` | `true`              | If the header based auth is enabled. |
| `stargate.header-based-auth.header-name` | `String`  | `X-Cassandra-Token` | Name of the authentication header.   |

#### [Metrics configuration](src/main/java/io/stargate/sgv2/docsapi/config/MetricsConfig.java)
*Configuration mapping for the additional metrics properties.*

| Property                                   | Type                 | Default | Description                                             |
|--------------------------------------------|----------------------|---------|---------------------------------------------------------|
| `stargate.metrics.global-tags`             | `Map<String,String>` | unset   | Map of global tags that will be applied to every meter. |


#### [Multi-tenancy configuration](src/main/java/io/stargate/sgv2/docsapi/config/TenantResolverConfig.java)
*Configuration mapping for the tenant resolving.*

| Property                                   | Type     | Default     | Description                                                                                                                         |
|--------------------------------------------|----------|-------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.tenant-resolver.type`            | `String` | `subdomain` | Tenant identifier resolver type. If unset, multi-tenancy is disabled. Possible options are `subdomain`, `fixed`, `custom` or unset. |
| `stargate.tenant-resolver.fixed.tenant-id` | `String` | unset       | Tenant identifier value if the `fixed` type is used.                                                                                |

#### [Cassandra token configuration](src/main/java/io/stargate/sgv2/docsapi/config/TokenResolverConfig.java)
*Configuration mapping for the Cassandra token resolving.*

| Property                                     | Type     | Default             | Description                                                                                                                |
|----------------------------------------------|----------|---------------------|----------------------------------------------------------------------------------------------------------------------------|
| `stargate.token-resolver.type`               | `String` | `principal`         | Token resolver type. If unset, empty token is used. Possible options are `header`, `principal` `fixed`, `custom` or unset. |
| `stargate.token-resolver.header.header-name` | `String` | `X-Cassandra-Token` | Header to get the token from if the `header` type is used.                                                                 |
| `stargate.token-resolver.fixed.token`        | `String` | unset               | Tenant identifier value if the `fixed` type is used.                                                                       |

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
In order to activate the tracing, you need to supply the OTLP gRPC endpoint using the JVM parameter `-Dquarkus.opentelemetry.tracer.exporter.otlp.endpoint=http://localhost:55680`.
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
