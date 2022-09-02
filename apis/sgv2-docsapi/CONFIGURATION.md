# Configuration Guide

> **IMPORTANT:** Please check the [Stargate Common Configuration](../sgv2-quarkus-common/CONFIGURATION.md) for properties shared between all Stargate V2 APIs.

## Stargate Docs API Configuration

### Document configuration
*Configuration for documents and their storage properties, defined by [DocumentConfig.java](src/main/java/io/stargate/sgv2/docsapi/config/DocumentConfig.java).*

| Property                                            | Type     | Default      | Description                                                   |
|-----------------------------------------------------|----------|--------------|---------------------------------------------------------------|
| `stargate.document.max-depth`                       | `int`    | `64`         | Max supported depth of a JSON document.                       |
| `stargate.document.max-array-length`                | `int`    | `1_000_000`  | Max supported single array length in a JSON document.         |
| `stargate.document.max-page-size`                   | `int`    | `20`         | The maximum page size when reading documents.                 |
| `stargate.document.max-search-page-size`            | `int`    | `1_000`      | The maximum Cassandra page size used when searching for documents. |
| `stargate.document.table.key-column-name`           | `String` | `key`        | The name of the column where a document key is stored.        |
| `stargate.document.table.leaf-column-name`          | `String` | `leaf`       | The name of the column where a JSON leaf name is stored.      |
| `stargate.document.table.string-value-column-name`  | `String` | `text_value` | The name of the column where a string value is stored.        |
| `stargate.document.table.double-value-column-name`  | `String` | `dbl_value`  | The name of the column where a double value is stored.        |
| `stargate.document.table.boolean-value-column-name` | `String` | `bool_value` | The name of the column where a boolean value is stored.       |
| `stargate.document.table.path-column-prefix`        | `String` | `p`          | The prefix of columns where JSON path part is saved.          |

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

However, here are some Stargate-relevant property groups that are important for correct setup of the service:

* `quarkus.grpc.clients.bridge` - property group for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - property group  for defining the keyspace cache used by [SchemaManager](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/schema/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)
