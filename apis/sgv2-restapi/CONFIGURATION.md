# Configuration Guide

> **IMPORTANT:** Please check the [Stargate Common Configuration](../sgv2-quarkus-common/CONFIGURATION.md) for properties shared between all Stargate V2 APIs.

## Stargate REST API Configuration

### Endpoint configuration

*Configuration for distinct endpoints*

| Property                     | Type      | Default | Description                                         |
|------------------------------|-----------|---------|-----------------------------------------------------|
| `stargate.rest.cql.disabled` | `boolean` | `true`  | Whether /v2/cql endpoint should be disabled or not. |

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

Here are some Stargate-relevant property groups that are necessary for correct service setup:

* `quarkus.grpc.clients.bridge` - property group for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - property group  for defining the keyspace cache used by [SchemaManager](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/schema/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)
