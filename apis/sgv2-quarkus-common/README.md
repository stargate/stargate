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
All requests must include the `X-Cassandra-Token` in order to be considered as authenticated.
The actual authentication and authorization is delegated to the Stargate V2 Bridge, by passing the value of that header as the Cassandra token.

The header-based authentication can be switched off and any [Quarkus Build-in Authentication Mechanism](https://quarkus.io/guides/security-built-in-authentication) can be used.
In that case,you need to provide a correct [CassandraTokenResolver](src/main/java/io/stargate/sgv2/api/common/token/CassandraTokenResolver.java) implementation, in order to resolve the C* token when calling the Stargate V2 Bridge.

The [Authentication & security configuration](CONFIGURATION.md#authentication--security-configuration) provides detailed view on available configuration options.

### Consistency levels

The common project enables configuration of the consistency levels used in Stargate APIs.
The consistency levels can be defined separately for queries doing:

1. schema changes
2. writes
3. reads

The [Queries configuration](CONFIGURATION.md#queries-configuration) provides detailed view on available configuration options.

### Metrics

All Stargate V2 APIs export Prometheus-based metrics.
The metrics collection is performed using the `quarkus-micrometer-registry-prometheus` extension, however the common module enables some extra metric configuration.

The [Metrics configuration](CONFIGURATION.md#metrics-configuration) provides detailed view on available configuration options.

### Multi-tenancy

The Stargate V2 explicitly supports the multi-tenancy.
Multi-tenancy is optional and is controlled by the configured [TenantResolver](src/main/java/io/stargate/sgv2/api/common/tenant/TenantResolver.java) implementation.
If a tenant is resolved for an HTTP request, the information about the tenant will be propagated to the Stargate V2 Bridge.
In addition, the tenant identifier could be added to metrics, traces, logs, etc.

The [Multi-tenancy configuration](CONFIGURATION.md#multi-tenancy-configuration) provides detailed view on available configuration options.

## Configuration properties

The common `stargate.` properties are defined by this project itself.
The properties are defined by dedicated config classes annotated with the `@ConfigMapping`.
The list of currently available properties is documented in the [Configuration Guide](CONFIGURATION.md).

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

> **_WARNING:_**  The common module is not made as a running application, but rather as a dependency used in the other modules.

### Running tests

The tests in this project can be run by:

```shell script
../mvnw test
```

In case you want to build without executing the tests you can do:

```shell script
../mvnw install -DskipUnitTests
```

> **_NOTE:_**  The common module is not containing integration tests.
