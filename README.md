[![Maven Central](https://img.shields.io/maven-central/v/io.stargate/stargate.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/io.stargate/stargate/)

# Stargate

An open source data API gateway.

Stargate is a data gateway deployed between client applications and a Cassandra database.
For developers, it makes it easy to use Cassandra for any application workload by adding plugin support for new APIs, data types, and access methods - even secondary database models. 
It's built with extensibility as a first-class citizen to enable rapid innovation. 
For operators, Stargate introduces microservice architecture, allowing independent deployment and scale of storage nodes, API Service nodes, and coordinator nodes in Cassandra clusters.

- For quick instructions on how to bring up Stargate on your desktop using Docker, check out the [Docker compose](docker-compose/README.md) instructions.
- If you wish to deploy in Kubernetes, there is also a [Helm chart](helm/README.md) you can use to install Stargate alongside an existing Cassandra cluster, or visit the [K8ssandra](https://k8ssandra.io) project for a distribution that packages Cassandra, Stargate, and additional tooling.
- For more information about how to deploy and use Stargate, visit [stargate.io](https://stargate.io/)
- To learn how to participate in our community, visit our [community page](https://stargate.io/community)
- To set up and use a Stargate development environment, visit the [dev guide](DEV_GUIDE.md)

## Contents
- [Introduction](#introduction)
- [Repositories](#repositories)
- [Issue Management](#issue-management)

## Introduction

We created Stargate because we got tired of using different databases and different APIs depending on the work that we were trying to get done.
With "read the manual" fatigue and lengthy selection processes wearing on us every time we started a new project, we thought - *why not create a framework that can serve many APIs for a range of workloads?*

This project enables customization of all aspects of data access and has modules for authentication, APIs, request handling / routing, and persistence backends.  
The current form is specific to Apache Cassandra (C*) compatible backends.

As shown in the figure below, Stargate is often deployed behind a load balancer or proxy and exposes multiple endpoints to client applications, including HTTP APIs, gRPC, and the Cassandra Query Language (CQL). Stargate sits in front of a Cassandra cluster which is used as the storage backend. 

![image](assets/stargate-arch-high-level.png#center)

Stargate consists of the following components, which we introduce briefly here with links to the corresponding modules in this monorepo.

### API Services
These are independently scalable microservices which various APIs, typically HTTP based. These modules can be found under the [apis](apis) directory: 

- [sgv2-restapi](apis/sgv2-restapi): API implementation for exposing Cassandra data over REST
- [sgv2-graphqlapi](apis/sgv2-graphqlapi): API implementation for exposing Cassandra data over GraphQL
- [sgv2-docsapi](apis/sgv2-docsapi): API implementation for exposing Cassandra data over a Document API

Each API Service contains its own integration test suite that tests it against the coordinator node and supported Cassandra backends. There is also a [sgv2-quarkus-common](apis/sgv2-quarkus-common) module containing utilities that may be used by all Java/Quarkus based API services.

**Warning:** Support for Cassandra 3.11 is considered deprecated and will be removed in the Stargate v3 release: [details](https://github.com/stargate/stargate/discussions/2242).

**Warning:** Support for Cassandra 3.11 is considered deprecated and will be removed in the Stargate v3 release: [details](https://github.com/stargate/stargate/discussions/2242).

- **Authentication Services**: Responsible for authentication to Stargate

### Coordinator Node
Coordinator nodes participate as non-data storing nodes in the backing Cassandra cluster, which enables them to read and write data more efficiently. Stargate Coordinator nodes can also be scaled independently. Coordinator nodes expose gRPC and CQL interfaces for fast access by client applications. The following are the key modules comprising the coordinator and its exposed interfaces:

- [core](coordinator/core): Common classes used throughout the other coordinator modules
- [cql](coordinator/cql): API implementation for the Cassandra Query Language
- [grpc](coordinator/grpc): fast CQL over gRPC implementation (HTTP-based interface equivalent to CQL performance)
- [bridge](coordinator/bridge): gRPC-based interface used by API services
- [health-checker](coordinator/health-checker): HTTP endpoints useful for health checking coordinator nodes
- [metrics-jersey](coordinator/metrics-jersey): metrics collection for the coordinator node and its exposed interfaces
- [stargate-starter](coordinator/stargate-starter): the main Java application used to start the coordinator via the `starctl` script

#### Persistence Services
Stargate coordinator nodes support a pluggable approach for implementing the coordination layer to execute requests passed by API services and other interfaces to underlying data storage instances. Persistence service implementations are responsible handling and converting requests to database queries, dispatching to a specific version of Cassandra, and returning and serving responses.

- [persistence-api](coordinator/persistence-api): Interface for working with persistence services 
- [persistence-common](coordinator/persistence-common): Utilities shared by the persistence services
- [persistence-cassandra-3.11](coordinator/persistence-cassandra-3.11): Joins C* 3.11 cluster as coordinator-only node (does not store data)
mocks C* system tables for native driver integration,
executes requests with C* storage nodes using C* QueryHandler/QueryProcessor,
converts internal C* objects and ResultSets to Stargate Datastore objects.
- [persistence-cassandra-4.0](coordinator/persistence-cassandra-4.0): (same as above but for Cassandra 4.0)
- [persistence-dse-6.8](coordinator/persistence-dse-6.8): (same as above but for DataStax Enterprise 6.8)

#### Authentication and Authorization Services
Stargate coordinator nodes also support a pluggable authentication and authorization approach.

- [authnz](coordinator/authnz): Interface for working with auth providers
- [auth-api](coordinator/auth-api): REST service for generating auth tokens
- [auth-table-based-service](coordinator/auth-table-based-service): Service to store tokens in the database
- [auth-jtw-service](coordinator/auth-jwt-service): Service to authenticate using externally generated JSON Web Tokens (JWTs)

#### Coordinator Node Testing
The following modules provide support for testing:

- [testing](coordinator/testing): Integration test suite for the coordinator node modules
- [testing](coordinator/testing-services): Testing helpers
- [persistence-test](coordinator/persistence-test): Common utilities for testing persistence services

Instructions for running and extending the test suite can be found in the [developer guide](DEV_GUIDE.md).

## Repositories

Here is an overview of the key repositories in the Stargate GitHub organization:

- [stargate/stargate](https://github.com/stargate/stargate): This repository is the primary entry point to the project. It is a monorepo containing all of the Stargate modules
- [stargate/docs](https://github.com/stargate/docs): This repository contains the user docs hosted on [stargate.io](https://stargate.io)
- [stargate/website](https://github.com/stargate/website): This repository contains the code for the website hosted on [stargate.io](https://stargate.io)

The organization also contains several gRPC client libraries for various languages.

## Issue Management

You can reference the [CONTRIBUTING.md](CONTRIBUTING.md) for a full description of how to get involved,
but the short of it is below.

- If you've found a bug (use the bug label) or want to request a new feature (use the enhancement label), file a GitHub issue
- If you're not sure about it or want to chat, reach out on our [Discord](https://discord.gg/GravUqY) 
- If you want to write some user docs 🎉 head over to the [stargate/docs](https://github.com/stargate/docs) repo, Pull Requests accepted!

## Supported Versions and Branching Strategy

The Stargate project maintains support for the current major version number and one major version number previous. We mark the previous major version number as deprecated in order to encourage usage of the latest version. We anticipate supporting a major version number release N for one year after the subsequent major version number release (N+1), or until the major version number release (N+2), whichever comes first. For example, v1 will be supported until when Stargate v3 is released, but no later than October 2023 (1 year after release date of Stargate v2). We anticipate producing a new major version number release every 6-12 months.

Supporting a release entails making bug fixes, keeping dependencies up to date, and making sure Docker images are free from vulnerabilities.

The current major version is maintained on the default `main` branch. The prior major version number is maintained on a version branch, for example, `v1`.

We make bug fixes in all supported releases. The recommended approach is to commit a fix first to the previous major version branch (if applicable) and merge it forward into the current major version branch.

We use minor version numbers to indicate any changes to the coordinator that would cause compatibility changes for Stargate APIs. For example, a breaking change to the [bridge](coordinator/bridge) API in v2.0.x would cause creation of a v2.1.0 release, and any API implementations would need to explicitly update in order to be compatible. You can use any version of the coordinator and API that have same the major and minor version number without issues.

We iterate forward rather than producing patch releases. For example, for a vulnerability found in `v2.0.3`, we'd make any required fixes and dependency updates and release `v2.0.4`. We maintain a regular release cadence of approximately twice a month but can iterate more quickly as the situation dictates. 


## Thanks

![YourKit Logo](https://www.yourkit.com/images/yklogo.png)

This project uses tools provided by YourKit, LLC. YourKit supports open source projects with its full-featured Java
Profiler. YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a> and
<a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>, innovative and intelligent tools for
profiling Java and .NET applications.
