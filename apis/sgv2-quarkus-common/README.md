# Stargate V2 Quarkus Common

This project holds shared functionality, configuration and concepts for all Stargate V2 APIs.

##### Table of Contents
* [Shared Concepts](#shared-concepts)
   * [Authentication](#authentication)
   * [Consistency](#consistency-levels) 
   * [Metrics](#metrics)
   * [Multi-tenancy](#multi-tenancy) 
* [Configuration properties](#configuration-properties)  
* [Development guide](#development-guide)  
   * [Running tests](#running-tests)

## Shared Concepts

### Authentication

By default, the APIs use the header-based authentication mechanism.
All requests must include the `X-Cassandra-Token`.
The actual authentication and authorization checks are delegated to the Stargate V2 Bridge, by passing the value of that header as the Cassandra token.

The header-based authentication can be switched off and any [Quarkus Build-in Authentication Mechanism](https://quarkus.io/guides/security-built-in-authentication) can be used.
In that case, you need to provide a correct [CassandraTokenResolver](src/main/java/io/stargate/sgv2/api/common/token/CassandraTokenResolver.java) implementation, in order to resolve the Cassandra token when calling the Stargate V2 Bridge.

The [Authentication & security configuration](CONFIGURATION.md#authentication--security-configuration) provides a detailed description of the available configuration options.

### Consistency levels

The common project enables configuration of the Cassandra consistency levels used in Stargate APIs.
The consistency levels can be defined separately for queries doing:

1. schema changes
2. writes
3. reads

The [Queries configuration](CONFIGURATION.md#queries-configuration) provides a detailed view of the available configuration options.

### Metrics

All Stargate V2 APIs export Prometheus-based metrics.
The metrics collection is performed using the `quarkus-micrometer-registry-prometheus` extension. The common module enables additional metric configuration.

The [Metrics configuration](CONFIGURATION.md#metrics-configuration) provides a detailed view of the available configuration options.

### Multi-tenancy

The Stargate V2 provides support for multi-tenancy.
Multi-tenancy is optional and is controlled by the configured [TenantResolver](src/main/java/io/stargate/sgv2/api/common/tenant/TenantResolver.java) implementation.
If a tenant is resolved for an HTTP request, the information about the tenant will be propagated to the Stargate V2 Bridge.
In addition, the tenant identifier can be used to tag metrics, or included in logs or traces.

The [Multi-tenancy configuration](CONFIGURATION.md#multi-tenancy-configuration) provides a detailed view of the available configuration options.

## Configuration properties

This project defines several common `stargate.` properties using dedicated config classes annotated with the `@ConfigMapping`.
The list of currently available properties is documented in the [Configuration Guide](CONFIGURATION.md).

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

We recommended that you install Quarkus CLI for the best development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

> **_WARNING:_**  The common module is not made as a running application, but rather as a dependency used in the other modules.

### Running tests

The tests in this project can be run using the command:

```shell script
../mvnw test
```

To build without executing the tests you can use this command:

```shell script
../mvnw install -DskipUnitTests
```

> **_NOTE:_**  The common module does not contain integration tests.
