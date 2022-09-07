# Stargate APIs

This is a root project for all Stargate V2 APIs.
The project (including its submodules) is currently an independent project in this monorepo, and it's not connected to the root `io.stargate:stargate` project.

## Development guide

The submodules in this project use Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

## Project Structure

| Project                                    | Description                                                                        |
|--------------------------------------------|------------------------------------------------------------------------------------|
| [sgv2-docsapi](sgv2-docsapi)               | The Stargate Docs API V2, enables document store in Cassandra.                     |
| [sgv2-restapi](sgv2-restapi)               | The Stargate REST API V2, enables RESTful access to Cassandra.                     |
| [sgv2-quarkus-common](sgv2-quarkus-common) | The common project for all APIs, holding shared functionality, configuration, etc. |
