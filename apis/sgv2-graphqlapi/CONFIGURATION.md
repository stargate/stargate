# Configuration Guide

> **IMPORTANT:** Please check the [Stargate Common Configuration](../sgv2-quarkus-common/CONFIGURATION.md) for properties shared between all Stargate V2 APIs.

## Stargate GraphQL API Configuration

### GraphQL configuration
*Configuration for GraphQL, defined by [GraphQLConfig.java](src/main/java/io/stargate/sgv2/graphql/config/GraphQLConfig.java).*

| Property                                   | Type      | Default             | Description                                                                                                                                                                                                               |
|--------------------------------------------|-----------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.graphql.enable-default-keyspace` | `boolean` | `true`              | Whether to default to the oldest keyspace when the user accesses `/graphql`. If this is disabled, `/graphql` throws an error, and the keyspace must be provided explicitly in the path, as in `/graphql/{keyspace_name}`. |
| `stargate.graphql.playground.enabled`      | `boolean` | `true`              | If GraphQL Playground is enabled at `/playground`.                                                                                                                                                                        |
| `stargate.graphql.playground.token-header` | `String`  | `X-Cassandra-Token` | Optional, the header name that carries the token that should auto-injected to the playground. Note that this is used as a fallback if `CassandraTokenResolver` can not resolve the token.                                 |

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

Here are some Stargate-relevant property groups that are necessary for correct service setup:

* `quarkus.grpc.clients.bridge` - property group for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - property group  for defining the keyspace cache used by [SchemaManager](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/schema/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)
