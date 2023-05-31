# Stargate APIs

The Maven project in this directory is the root project for Stargate APIs that run externally to the coordinator node.  
The project (including its submodules) is an independent project in this monorepo, not connected to the root `io.stargate:stargate` project.

The REST, GraphQL, and Docs API services were factored into independent services as part of the Stargate v2 effort.

## Quickstart

The fastest way to build your own copy of the Stargate API service Docker images involves the following steps:

* Make sure you are in the `apis` directory and have `JAVA_HOME` set to point to a JDK 17 installation
* Do a Quarkus build including the option to generate images:
  ```
  ./mvnw clean package -DskipTests -Dquarkus.container-image.build=true
  ```
* Verify the images exist
  ```
  docker images | grep <stargate version>
  ```    

You can then use the [docker-compose](../docker-compose) scripts to start Stargate locally. See also the [coordinator README](../coordinator/README.md#quickstart) for information on compiling and building images for the coordinator.

## Development guide

The submodules in this project use Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

## Project Structure

| Project                                    | Description                                                                        |
|--------------------------------------------|------------------------------------------------------------------------------------|
| [sgv2-quarkus-common](sgv2-quarkus-common) | The common project for all APIs, holding shared functionality, configuration, etc. |
| [sgv2-docsapi](sgv2-docsapi)               | The Stargate Docs API provides a document store on top of Cassandra.               |
| [sgv2-graphqlapi](sgv2-graphqlapi)         | The Stargate GraphQL API provides access to Cassandra data using GraphQL queries.  |
| [sgv2-restapi](sgv2-restapi)               | The Stargate REST API provides an HTTP interface for accessing Cassandra data.     |