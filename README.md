![Java CI](https://github.com/stargate/stargate/workflows/Java%20CI/badge.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.stargate/stargate.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/io.stargate/stargate/)

# Stargate

An open source API framework for data.

Stargate is a data gateway deployed between client applications and a database.
It's built with extensibility as a first-class citizen and makes it easy to use a database for any application workload
by adding plugin support for new APIs, data types, and access methods.

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

This project enables customization of all aspects of data access and has modules for authentication, APIs, request handling / routing, and persistence backends.
The current form is specific to the Apache Cassandra (C*) backend but there's no bounds to the databases or APIs that this framework can support.

Stargate contains the following components:

- **API Services**: Responsible for defining the API, handling and converting requests to db queries, dispatching to persistence, returning and serving response

    - cql: API implementation for the Cassandra Query Language
    - restapi: API implementation for exposing Cassandra data over REST

- **Persistence Services**: Responsible for implementing the coordination layer to execute requests passed by API services to underlying data storage instances.

    - persistence-api: Interface for working with persistence services
    - persistence-common: Utilities shared by the persistence services
    - persistence-cassandra-3.11: Joins C* ring as coordinator only node (does not store data),
    mocks C* system tables for native driver integration,
    executes requests with C* storage nodes using C* QueryHandler/QueryProcessor,
    converts internal C* objects and ResultSets to Stargate Datastore objects.

- **Authentication Services**: Responsible for authentication to Stargate

    - auth-api: REST service for generating auth tokens
    - auth-table-based-service: Service to store tokens in the database
    - authentication: Interface for working with auth providers

![image](assets/stargate-modules-preview-version.png#center)

## Repositories

- [stargate/stargate](https://github.com/stargate/stargate): This repository is the primary entry point to the project. It contains the core set of modules for a fully functional starter experience.
- [stargate/api-extensions](https://github.com/stargate/api-extensions): This repository contains API Extensions and is where new API submissions should live (more coming soon). The REST API is in the stargate/stargate repo as a reference and to ease the starter experience.
- [stargate/persistence-extensions](https://github.com/stargate/persistence-extensions): This repository contains Persistence Extensions and is where new Persistence submissions should live (more coming soon). The Cassandra 3.11 persistence service is in the stargate/stargate repo as a reference and to ease the starter experience.
- [stargate/docker-images](https://github.com/stargate/docker-images): This repository contains the Dockerfiles used to create and publish images to https://hub.docker.com/orgs/stargateio
- [stargate/docs](https://github.com/stargate/docs): This repository contains the user docs hosted on [stargate.io](https://stargate.io)
- [stargate/website](https://github.com/stargate/website): This repository contains the code for the website hosted on [stargate.io](https://stargate.io)

## Issue Management

We're still getting things organized so bear with us if things move around a bit as we get settled.
You can reference the [CONTRIBUTING.md](CONTRIBUTING.md) for a full description of how to get involved
but the short of it is below.

- If you've found a bug (use the bug label) or want to request a new feature (use the enhancement label), file a GitHub issue
- If you're not sure about it or want to chat, reach out on our [mailing list](https://groups.google.com/a/lists.stargate.io/g/stargate-users)
- If you want to write some user docs 🎉 head over to the [stargate/docs](https://github.com/stargate/docs) repo, Pull Requests accepted!
