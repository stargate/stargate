# Stargate REST API (Stargate V2)

This project represents the stand-alone REST API microservice for Stargate V2, extracted from monolithic Stargate V1 Coordinator.
REST API is an HTTP service that allows access to data stored in a Cassandra cluster using RESTful interface.

The project depends on [sgv2-quarkus-common](../sgv2-quarkus-common) module, which defines general common project for all Stargate
V2 APIs.

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

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:9092/stargate/dev/.

## Configuration properties

There are two main configuration property prefixes used, `stargate.` and `quarkus.`.
Latter covers configuration of Quarkus-provided standard features, former Stargate-specific features.

Configuration settings can be found from `application.yaml` files located in:

* `src/main/resources/application.yaml`: Production settings, used as the baseline for other modes
* `src/test/resources/application.yaml`: Overrides for "test" phase
