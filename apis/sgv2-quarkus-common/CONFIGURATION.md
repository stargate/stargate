# Configuration Guide

## Stargate Common Configuration

### Authentication & security configuration
*Configuration for header-based authentication, defined by [AuthConfig.java](src/main/java/io/stargate/sgv2/api/common/config/AuthConfig.java).*

| Property                                              | Type      | Default                                      | Description                                                                                                                |
|-------------------------------------------------------|-----------|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `stargate.auth.header-based.enabled`                  | `boolean` | `true`                                       | If the header based auth is enabled.                                                                                       |
| `stargate.auth.header-based.header-name`              | `String`  | `X-Cassandra-Token`                          | Name of the authentication header.                                                                                         |
| `stargate.auth.header-based.custom-challenge-enabled` | `String`  | `${stargate.exception-mappers.enabled:true}` | Provides possibility to disable the customized auth challenge defined.                                                     |
| `stargate.auth.token-resolver.type`                   | `String`  | `principal`                                  | Token resolver type. If unset, empty token is used. Possible options are `header`, `principal` `fixed`, `custom` or unset. |
| `stargate.auth.token-resolver.header.header-name`     | `String`  | `X-Cassandra-Token`                          | Header to get the token from, if the `header` type is used.                                                                |
| `stargate.auth.token-resolver.fixed.token`            | `String`  | unset                                        | Tenant identifier value, if the `fixed` type is used.                                                                      |

### Data store configuration
*Configuration of the data-store properties, defined by [DataStoreConfig.java](src/main/java/io/stargate/sgv2/api/common/config/DataStoreConfig.java).*

| Property                                        | Type      | Default                             | Description                                                                                                                                            |
|-------------------------------------------------|-----------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.data-store.ignore-bridge`             | `boolean` | `${stargate.multi-tenancy.enabled}` | If the data store supported features cannot be read from the Bridge. In case of a failure to read, the properties listed below are taken as fall back. |
| `stargate.data-store.secondary-indexes-enabled` | `boolean` | `true`                              | If the data store supports secondary indexes.                                                                                                          |
| `stargate.data-store.sai-enabled`               | `boolean` | `false`                             | If the data store supports SAI (storage-attached indexes).                                                                                             |
| `stargate.data-store.logged-batches-enabled`    | `boolean` | `true`                              | If the data store supports logged batches.                                                                                                             |
| `stargate.data-store.vector-search-enabled`     | `boolean` | `true`                              | If the data store supports vector search.                                                                                                              |

### gRPC configuration
*Configuration for the gRPC calls to the Bridge, defined by [GrpcConfig.java](src/main/java/io/stargate/sgv2/api/common/config/GrpcConfig.java).*

| Property                             | Type       | Default        | Description                                                                                                                         |
|--------------------------------------|------------|----------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.grpc.call-deadline`        | `Duration` | `PT30S`        | Defines the client deadline for each RPC call to the bridge.                                                                        |
| `stargate.grpc.retries.enabled`      | `boolean`  | `true`         | If retries of bridge calls is enabled.                                                                                              |
| `stargate.grpc.retries.policy`       | `String`   | `status-codes` | Retry policy type. Possible options are `status-codes` or `custom`.                                                                 |
| `stargate.grpc.retries.status-codes` | `List`     | `UNAVAILABLE`  | In case of a `status-codes` policy, provides a list of gRPC `Status.Code`s that must be returned in order for a call to be retried. |
| `stargate.grpc.retries.max-attempts` | `int`      | `1`            | Maximum amount of retry attempts for a single call.                                                                                 |

### gRPC metadata configuration
*Configuration for the gRPC metadata passed to the Bridge, defined by [GrpcMetadataConfig.java](src/main/java/io/stargate/sgv2/api/common/config/GrpcMetadataConfig.java).*

| Property                                     | Type     | Default             | Description                                                 |
|----------------------------------------------|----------|---------------------|-------------------------------------------------------------|
| `stargate.grpc-metadata.tenant-id-key`       | `String` | `X-Tenant-Id`       | Metadata key for passing the tenant-id to the Bridge.       |
| `stargate.grpc-metadata.cassandra-token-key` | `String` | `X-Cassandra-Token` | Metadata key for passing the cassandra token to the Bridge. |
| `stargate.grpc-metadata.source-api-key`      | `String` | `X-Source-Api`      | Metadata key for passing the source API to the Bridge.      |

### Metrics configuration
*Configuration mapping for the additional metrics properties, defined by [MetricsConfig.java](src/main/java/io/stargate/sgv2/api/common/config/MetricsConfig.java).*

| Property                                                         | Type                 | Default                             | Description                                                                                                                                                  |
|------------------------------------------------------------------|----------------------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.metrics.global-tags`                                   | `Map<String,String>` | `{"module": "docsapi"}`             | Map of global tags that will be applied to every meter.                                                                                                      |
| `stargate.metrics.tenant-request-counter.enabled`                | `boolean`            | `${stargate.multi-tenancy.enabled}` | If extra metric for counting the request per tenant should be recorded.                                                                                      |
| `stargate.metrics.tenant-request-counter.metric-name`            | `String`             | `http.server.requests.counter`      | Name of the metric.                                                                                                                                          |
| `stargate.metrics.tenant-request-counter.tenant-tag`             | `String`             | `tenant`                            | The tag key for tenant id.                                                                                                                                   |
| `stargate.metrics.tenant-request-counter.error-tag`              | `String`             | `error`                             | The tag key for the request error flag (true/false).                                                                                                         |
| `stargate.metrics.tenant-request-counter.user-agent-tag`         | `String`             | `user_agent`                        | The tag key for the user agent, if capturing is enabled.                                                                                                     |
| `stargate.metrics.tenant-request-counter.user-agent-tag-enabled` | `boolean`            | `false`                             | If user agent information should be included as a tag. The user agent value will be trimmed to the first appearance of the whitespace or forward slash char. |

### Multi-tenancy configuration
*Configuration mapping for multi tenant operation mode, defined by [MultiTenancyConfig.java](src/main/java/io/stargate/sgv2/api/common/config/MultiTenancyConfig.java).*

| Property                                                     | Type      | Default | Description                                                                                                                                        |
|--------------------------------------------------------------|-----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `stargate.multi-tenancy.enabled`                             | `boolean` | `false` | If multi-tenancy operation mode is enabled.                                                                                                        |
| `stargate.multi-tenancy.tenant-resolver.type`                | `String`  | unset   | Tenant identifier resolver type if multi-tenancy is enabled. Possible options are `subdomain`, `fixed`, or `custom`.                               |
| `stargate.multi-tenancy.tenant-resolver.fixed.tenant-id`     | `String`  | unset   | Tenant identifier value if the `fixed` type is used.                                                                                               |
| `stargate.multi-tenancy.tenant-resolver.subdomain.max-chars` | `String`  | unset   | Optional, takes only a defined number of max characters from the resolved sub-domain as tenant id. If config value is negative, then it's ignored. |
| `stargate.multi-tenancy.tenant-resolver.subdomain.regex`     | `String`  | unset   | Optional, validates the resolved tenant id against the provided regular expression. If not matching the regex, resolves to empty (unknown) tenant. |

### Queries configuration
*Configuration mapping for the query properties, defined by [QueriesConfig.java](src/main/java/io/stargate/sgv2/api/common/config/QueriesConfig.java).*

| Property                                      | Type     | Default        | Description                                                                      |
|-----------------------------------------------|----------|----------------|----------------------------------------------------------------------------------|
| `stargate.queries.consistency.schema-changes` | `String` | `LOCAL_QUORUM` | Consistency level to use for C* queries that are performing the schema changes.  |
| `stargate.queries.consistency.writes`         | `String` | `LOCAL_QUORUM` | Consistency level to use for C* queries that are inserting or updating the data. |
| `stargate.queries.consistency.reads`          | `String` | `LOCAL_QUORUM` | Consistency level to use for C* queries that are reading the data.               |
| `stargate.queries.serial-consistency`         | `String` | `SERIAL`      | Serial consistency level to be used for C* queries.                              |

## Stargate Development Configuration

> NOTE: This properties cre usually helpful when you are using `sgv2-quarkus-common` to develop custom APIs.

### Exception mappers

| Property                             | Type      | Default | Description                                                                                                                       |
|--------------------------------------|-----------|---------|-----------------------------------------------------------------------------------------------------------------------------------|
| `stargate.exception-mappers.enabled` | `boolean` | `unset` | If set to `false` it disables all common exception mappers defined. You would need to define your own exception mapping strategy. |
