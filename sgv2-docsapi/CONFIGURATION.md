# Configuration Guide

## Stargate Configuration

### [Data store configuration](src/main/java/io/stargate/sgv2/docsapi/config/DataStoreConfig.java)
*Configuration of the data-store properties.*

| Property                                        | Type      | Default | Description                                                                                                                            |
|-------------------------------------------------|-----------|---------|----------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.data-store.read-from-brdge`           | `boolean` | `false` | If the data store properties should be read from the Bridge. I case of a failure to read, the below properties are taken as fall back. |
| `stargate.data-store.secondary-indexes-enabled` | `boolean` | `true`  | If the data store supports secondary indexes.                                                                                          |
| `stargate.data-store.sai-enabled`               | `boolean` | `false` | If the data store supports SAI (storage-attached indexes).                                                                             |
| `stargate.data-store.logged-batches-enabled`    | `boolean` | `true`  | If the data store supports logged batches.                                                                                             |

### [Document configuration](src/main/java/io/stargate/sgv2/docsapi/config/DocumentConfig.java)
*Configuration for the documents and their storage properties.*

| Property                                            | Type     | Default      | Description                                                   |
|-----------------------------------------------------|----------|--------------|---------------------------------------------------------------|
| `stargate.document.max-dept`                        | `int`    | `64`         | Max supported depth of a JSON document.                       |
| `stargate.document.max-array-length`                | `int`    | `1_000_000`  | Max supported single array length in a JSON document.         |
| `stargate.document.max-page-size`                   | `int`    | `20`         | The maximum page size when reading documents.                 |
| `stargate.document.search-page-size`                | `int`    | `1_000`      | Defines the Cassandra page size when searching for documents. |
| `stargate.document.table.key-column-name`           | `String` | `key`        | The name of the column where a document key is stored.        |
| `stargate.document.table.leaf-column-name`          | `String` | `leaf`       | The name of the column where a JSON leaf name is stored.      |
| `stargate.document.table.string-value-column-name`  | `String` | `text_value` | The name of the column where a string value is stored.        |
| `stargate.document.table.double-value-column-name`  | `String` | `dbl_value`  | The name of the column where a double value is stored.        |
| `stargate.document.table.boolean-value-column-name` | `String` | `bool_value` | The name of the column where a boolean value is stored.       |
| `stargate.document.table.path-column-prefix`        | `String` | `p`          | The prefix of columns where JSON path part is saved.          |


### [gRPC metadata configuration](src/main/java/io/stargate/sgv2/docsapi/config/GrpcMetadataConfig.java)
*Configuration for the gRPC metadata passed to the Bridge.*

| Property                                     | Type     | Default             | Description                                                 |
|----------------------------------------------|----------|---------------------|-------------------------------------------------------------|
| `stargate.grpc-metadata.tenant-id-key`       | `String` | `X-Tenant-Id`       | Metadata key for passing the tenant-id to the Bridge.       |
| `stargate.grpc-metadata.cassandra-token-key` | `String` | `X-Cassandra-Token` | Metadata key for passing the cassandra token to the Bridge. |

### [Header-based authentication configuration](src/main/java/io/stargate/sgv2/docsapi/config/HeaderBasedAuthConfig.java)
*Configuration for the header based authentication.*

| Property                                 | Type      | Default             | Description                          |
|------------------------------------------|-----------|---------------------|--------------------------------------|
| `stargate.header-based-auth.enabled`     | `boolean` | `true`              | If the header based auth is enabled. |
| `stargate.header-based-auth.header-name` | `String`  | `X-Cassandra-Token` | Name of the authentication header.   |

### [Multi-tenancy configuration](src/main/java/io/stargate/sgv2/docsapi/config/TenantResolverConfig.java)
*Configuration mapping for the tenant resolving.*

| Property                                   | Type     | Default     | Description                                                                                                                         |
|--------------------------------------------|----------|-------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.tenant-resolver.type`            | `String` | `subdomain` | Tenant identifier resolver type. If unset, multi-tenancy is disabled. Possible options are `subdomain`, `fixed`, `custom` or unset. |
| `stargate.tenant-resolver.fixed.tenant-id` | `String` | unset       | Tenant identifier value if the `fixed` type is used.                                                                                |

### [Cassandra token configuration](src/main/java/io/stargate/sgv2/docsapi/config/TokenResolverConfig.java)
*Configuration mapping for the Cassandra token resolving.*

| Property                                     | Type     | Default             | Description                                                                                                                |
|----------------------------------------------|----------|---------------------|----------------------------------------------------------------------------------------------------------------------------|
| `stargate.token-resolver.type`               | `String` | `principal`         | Token resolver type. If unset, empty token is used. Possible options are `header`, `principal` `fixed`, `custom` or unset. |
| `stargate.token-resolver.header.header-name` | `String` | `X-Cassandra-Token` | Header to get the token from if the `header` type is used.                                                                 |
| `stargate.token-resolver.fixed.token`        | `String` | unset               | Tenant identifier value if the `fixed` type is used.                                                                       |

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.
