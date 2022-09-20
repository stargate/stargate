[![Maven Central](https://img.shields.io/maven-central/v/io.stargate/stargate.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/io.stargate/stargate/)

# Stargate

An open source data API gateway.

Stargate is a data gateway deployed between client applications and a Cassandra database.
For developers, it makes it easy to use Cassandra for any application workload by adding plugin support for new APIs, data types, and access methods - even secondary database models. It's built with extensibility as a first-class citizen to enable rapid innovation. For operators, stargate introduces microservice architecture, allowing independent deployment and scale of storage nodes, API Service nodes, and coordinator nodes in Cassandra clusters.

- For information about how to use Stargate, visit [stargate.io](https://stargate.io/)
- To learn how to participate in our community, visit our [community page](https://stargate.io/community)
- To set up and use a Stargate development environment, visit the [dev guide](DEV_GUIDE.md)

![image](assets/stargate-arch-high-level.png#center)

## Contents
- [Introduction](#introduction)
- [Repositories](#repositories)
- [Issue Management](#issue-management)

## Introduction

We created Stargate because we got tired of using different databases and different APIs depending on the work that we were trying to get done.
With "read the manual" fatigue and lengthy selection processes wearing on us every time we started a new project, we thought - *why not create a framework that can serve many APIs for a range of workloads?*

This project enables customization of all aspects of data access and has modules for authentication, APIs, request handling / routing, and persistence backends.  The current form is specific to the Apache Cassandra (C*) backend but there's no bounds to the databases or APIs that this framework can support.

Stargate contains the following components:

- **API Services**: Responsible for defining the API, handling and converting requests to db queries, dispatching to persistence, returning and serving response

    - cql: API implementation for the Cassandra Query Language
    - restapi: API implementation for exposing Cassandra data over REST
    - graphqlapi: API implementation for exposing Cassandra data over GraphQL

- **Persistence Services**: Responsible for implementing the coordination layer to execute requests passed by API services to underlying data storage instances.

    - persistence-api: Interface for working with persistence services
    - persistence-common: Utilities shared by the persistence services
    - persistence-cassandra-3.11: Joins C* 3.11 cluster as coordinator-only node (does not store data),
    mocks C* system tables for native driver integration,
    executes requests with C* storage nodes using C* QueryHandler/QueryProcessor,
    converts internal C* objects and ResultSets to Stargate Datastore objects.
    - persistence-cassandra-4.0: (same as above but for Cassandra 4.0)

- **Authentication Services**: Responsible for authentication to Stargate

    - auth-api: REST service for generating auth tokens
    - auth-table-based-service: Service to store tokens in the database
    - authentication: Interface for working with auth providers

![image](assets/stargate-modules-preview-version.png#center)

## Repositories

- [stargate/stargate](https://github.com/stargate/stargate): This repository is the primary entry point to the project. It contains all of the modules.
- [stargate/docker-images](https://github.com/stargate/docker-images): This repository contains the Dockerfiles used to create and publish images to https://hub.docker.com/orgs/stargateio
- [stargate/docs](https://github.com/stargate/docs): This repository contains the user docs hosted on [stargate.io](https://stargate.io)
- [stargate/website](https://github.com/stargate/website): This repository contains the code for the website hosted on [stargate.io](https://stargate.io)

## Issue Management

You can reference the [CONTRIBUTING.md](CONTRIBUTING.md) for a full description of how to get involved,
but the short of it is below.

- If you've found a bug (use the bug label) or want to request a new feature (use the enhancement label), file a GitHub issue
- If you're not sure about it or want to chat, reach out on our [Discord](https://discord.gg/GravUqY) 
- If you want to write some user docs 🎉 head over to the [stargate/docs](https://github.com/stargate/docs) repo, Pull Requests accepted!

## Thanks

![YourKit Logo](https://www.yourkit.com/images/yklogo.png)

This project uses tools provided by YourKit, LLC. YourKit supports open source projects with its full-featured Java
Profiler. YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a> and
<a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>, innovative and intelligent tools for
profiling Java and .NET applications.
