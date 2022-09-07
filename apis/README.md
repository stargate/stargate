# Stargate APIs

The Maven project in this directory is the root project for Stargate APIs that run externally to the coordinator node.  
The project (including its submodules) is an independent project in this monorepo, not connected to the root `io.stargate:stargate` project.

The REST, GraphQL, and Docs API services were factored into independent services as part of the Stargate v2 effort.

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
| [sgv2-docsapi](sgv2-docsapi)               | The Stargate Docs API provides a document store on top of Cassandra. |
| [sgv2-graphqlapi](sgv2-graphqlapi)         | The Stargate GraphQL API provides access to Cassandra data using GraphQL queries.  |
| [sgv2-restapi](sgv2-restapi)               | The Stargate REST API provides an HTTP interface for accessing Cassandra data.  |