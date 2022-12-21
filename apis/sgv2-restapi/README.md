# Stargate REST API (Stargate V2)

This project represents the stand-alone REST API microservice for Stargate V2, extracted from monolithic Stargate V1 Coordinator.
REST API is an HTTP service that allows access to data stored in a Cassandra cluster using RESTful interface.

The project depends on [sgv2-quarkus-common](../sgv2-quarkus-common) module, which defines general common project for all Stargate V2 APIs.

All issues related to this project are marked with the `stargate-v2` and `REST` [labels](https://github.com/\
stargate/stargate/issues?q=is%3Aopen+is%3Aissue+label%3Astargate-v2+label%3AREST).

REST API runs as a [Quarkus](https://quarkus.io/) service, different from Stargate V1 APIs
which was build on DropWizard framework.

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

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

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8082/stargate/dev/.

## Configuration properties

There are two main configuration property prefixes used, `stargate.` and `quarkus.`.

The `quarkus.` properties are defined by the Quarkus framework, and the complete list of available properties can be found on the [All configuration options](https://quarkus.io/guides/all-config) page.
In addition, the related guide of each [Quarkus extension](#quarkus-extensions) used in the project provides an overview of the available config options.

The `stargate.` properties are defined by this project itself and by the [sgv2-quarkus-common configuration](../sgv2-quarkus-common/CONFIGURATION.md).
The list of currently available REST API properties is documented in the [Configuration Guide](CONFIGURATION.md).

Configuration settings can be found from `application.yaml` files located in:

* `src/main/resources/application.yaml`: Production settings, used as the baseline for other modes
* `src/test/resources/application.yaml`: Overrides for "test" phase

## Quarkus Extensions

This project uses various Quarkus extensions, modules that run on top of a Quarkus application.
You can list, add and remove the extensions using the `quarkus ext` command.

> *NOTE: Please check the shared extensions introduced by the [sgv2-quarkus-common](../sgv2-quarkus-common/README.md#shared-quarkus-extensions) project.

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
