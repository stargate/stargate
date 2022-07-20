# Stargate REST API (Stargate V2)

This project represents the stand-alone REST API microservice for Stargate V2, extracted from monolithic Stargate V1 Coordinator.
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

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8180/stargate/dev/.
## Configuration properties

There are two main configuration property prefixes used, `stargate.` and `quarkus.`.
Latter covers configuration of Quarkus-provided standard features, former Stargate-specific features.

(TO BE COMPLETED)

### Configuration: General Quarkus properties

(TO BE WRITTEN)

### Configuration: Stargate-specific properties

(TO BE WRITTEN)
