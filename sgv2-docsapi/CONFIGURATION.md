# Configuration Guide

## Stargate Configuration

### [Authentication & security configuration](src/main/java/io/stargate/sgv2/docsapi/config/AuthConfig.java)
*Configuration for the header based authentication.*

| Property                                          | Type      | Default             | Description                                                                                                                |
|---------------------------------------------------|-----------|---------------------|----------------------------------------------------------------------------------------------------------------------------|
| `stargate.auth.header-based.enabled`              | `boolean` | `true`              | If the header based auth is enabled.                                                                                       |
| `stargate.auth.header-based.header-name`          | `String`  | `X-Cassandra-Token` | Name of the authentication header.                                                                                         |
| `stargate.auth.token-resolver.type`               | `String`  | `principal`         | Token resolver type. If unset, empty token is used. Possible options are `header`, `principal` `fixed`, `custom` or unset. |
| `stargate.auth.token-resolver.header.header-name` | `String`  | `X-Cassandra-Token` | Header to get the token from, if the `header` type is used.                                                                |
| `stargate.auth.token-resolver.fixed.token`        | `String`  | unset               | Tenant identifier value, if the `fixed` type is used.                                                                      |

### [Data store configuration](src/main/java/io/stargate/sgv2/docsapi/config/DataStoreConfig.java)
*Configuration of the data-store properties.*

| Property                                        | Type      | Default                             | Description                                                                                                                                        |
|-------------------------------------------------|-----------|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.data-store.ignore-bridge`             | `boolean` | `${stargate.multi-tenancy.enabled}` | If the data store supported features should not be read from the Bridge. I case of a failure to read, the below properties are taken as fall back. |
| `stargate.data-store.secondary-indexes-enabled` | `boolean` | `true`                              | If the data store supports secondary indexes.                                                                                                      |
| `stargate.data-store.sai-enabled`               | `boolean` | `false`                             | If the data store supports SAI (storage-attached indexes).                                                                                         |
| `stargate.data-store.logged-batches-enabled`    | `boolean` | `true`                              | If the data store supports logged batches.                                                                                                         |

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

### [Metrics configuration](src/main/java/io/stargate/sgv2/docsapi/config/MetricsConfig.java)
*Configuration mapping for the additional metrics properties.*

| Property                                              | Type                 | Default                             | Description                                                             |
|-------------------------------------------------------|----------------------|-------------------------------------|-------------------------------------------------------------------------|
| `stargate.metrics.global-tags`                        | `Map<String,String>` | `{"module": "docsapi"}`             | Map of global tags that will be applied to every meter.                 |
| `stargate.metrics.tenant-request-counter.enabled`     | `boolean`            | `${stargate.multi-tenancy.enabled}` | If extra metric for counting the request per tenant should be recorded. |
| `stargate.metrics.tenant-request-counter.metric-name` | `String`             | `http.server.requests.counter`      | Name of the metric.                                                     |
| `stargate.metrics.tenant-request-counter.tenant-tag`  | `String`             | `tenant`                            | The tag key for tenant id.                                              |
| `stargate.metrics.tenant-request-counter.error-tag`   | `String`             | `error`                             | The tag key for the request error flag (true/false).                    |

### [Multi-tenancy configuration](src/main/java/io/stargate/sgv2/docsapi/config/MultiTenancyConfig.java)
*Configuration mapping for multi tenant operation mode.*

| Property                                                 | Type      | Default | Description                                                                                                          |
|----------------------------------------------------------|-----------|---------|----------------------------------------------------------------------------------------------------------------------|
| `stargate.multi-tenancy.enabled`                         | `boolean` | `false` | If multi-tenancy operation mode is enabled.                                                                          |
| `stargate.multi-tenancy.tenant-resolver.type`            | `String`  | unset   | Tenant identifier resolver type if multi-tenancy is enabled. Possible options are `subdomain`, `fixed`, or `custom`. |
| `stargate.multi-tenancy.tenant-resolver.fixed.tenant-id` | `String`  | unset   | Tenant identifier value if the `fixed` type is used.                                                                 |

### [Queries configuration](src/main/java/io/stargate/sgv2/docsapi/config/QueriesConfig.java)
*Configuration mapping for the query properties.*

| Property                                      | Type     | Default        | Description                                                                      |
|-----------------------------------------------|----------|----------------|----------------------------------------------------------------------------------|
| `stargate.queries.consistency.schema-changes` | `String` | `LOCAL_QUORUM` | Consistency level to use for C* queries that are performing the schema changes.  |
| `stargate.queries.consistency.writes`         | `String` | `LOCAL_QUORUM` | Consistency level to use for C* queries that are inserting or updating the data. |
| `stargate.queries.consistency.reads`          | `String` | `LOCAL_QUORUM` | Consistency level to use for C* queries that are reading the data.               |

## Quarkus Configuration

The complete list of Quarkus available properties can be found on [All configuration options](https://quarkus.io/guides/all-config) page.

However, here are some Stargate-relevant properties groups that are important for correct setup of the service:

* `quarkus.grpc.clients.bridge` - group of properties for defining the Bridge gRPC client (see [gRPC Client configuration](https://quarkus.io/guides/grpc-service-consumption#client-configuration) for all options)
* `quarkus.cache.caffeine.keyspace-cache` - group of properties for defining the keyspace cache used by [SchemaManager](src/main/java/io/stargate/sgv2/docsapi/service/schema/common/SchemaManager.java) (see [Caffeine cache configuration](https://quarkus.io/guides/cache#caffeine-configuration-properties) for all options)
