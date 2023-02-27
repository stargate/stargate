# Stargate Metrics Jersey

This module provides additional possibilities to collect metrics for HTTP-based Stragate APIs.
The motivation behind this module is to overcome limitations the Dropwizard metrics has, concretely, not being able to have tags in metrics.


This module is built on top of the [Micrometer](https://github.com/micrometer-metrics/micrometer) library, that allows flexible metrics and tags definitions.
All the metrics collected are exposed using Prometheus, together with Dropwizard metrics.
Due to the OSGi, the join of the Micrometer and Dropwizard metrics is done in the [MetricsImpl](../core/src/main/java/io/stargate/core/metrics/impl/MetricsImpl.java) class, which is part of the [core](../core) module.

## Usage

This module is utilized by [MetricsBinder](src/main/java/io/stargate/metrics/jersey/MetricsBinder.java) class, that can register available application listeners that monitor requests and collect metrics.
Any module can initialize the class and pass the *Jersey environment* in order to register listeners.

The module provides two listeners:

1. [Metering listener](#metering-listener) - that meters HTTP request execution times
2. [Counting listener](#counting-listener) - that counts HTTP requests

Both listeners are enabled by default and can be independently [configured](#listeners-configuration).

## Metrics

### Metering listener

Metering listener collect metrics about the HTTP request execution times, and exposes them in the following metrics:

* `http_server_request_seconds_count`
* `http_server_request_seconds_sum`
* `http_server_request_seconds_max`

Beside the [default module tags](#default-tags), this listener always adds HTTP method, status and URI tags for each request.
This listener also enables collecting of [percentiles](#percentiles).

### Counting listener

Counting listener counts the HTTP requests, and exposes them in the following metric:

* `http_server_request_counter_total`

Beside the [default module tags](#default-tags), this listener always adds the HTTP error tag for each request.

## Percentiles

In order to collect percentiles for HTTP request execution times, you can use a [configuration option](#percentiles-configuration):

```bash
-Dstargate.metrics.http_server_requests_percentiles=0.99
```

This will create additional metric, `http_server_requests_seconds`, and will have percentiles as a tag in the form `{quantile="0.99"}`.

## Tags

This module provides a very flexible way of defining which tags will be added to all metrics being collected, in addition to the ones defined by the listeners.

### Default tags

The module name is always included in the collected metrics.
Modules can pass it symbolic name to the [MetricsBinder](src/main/java/io/stargate/metrics/jersey/MetricsBinder.java) constructor.

### Path parameter tags

It is possible to extract variable path parameters from the request URI and add those as tags.
This tag provider is enabled by specifying the name of the variable used in the HTTP path definition.

For example, having a path defined in a resource class as:

```java
@Path("/v2/example/{some-param}")
```

and specifying a [configuration option](#tags-configuration):

```bash
-Dstargate.metrics.http_server_requests_path_param_tags=some-param
```

will extract real path value from a request and contribute to the tags in the form `{some-param="value"}`.

### Header tags

It is possible to extract request headers and contribute their values as tags.
This tag provider is enabled by specifying the name of the HTTP header(s) using a [configuration option](#tags-configuration):

```bash
-Dstargate.metrics.http_server_requests_header_tags=x-forwarded-host
```

Note that header names are case-insensitive.
If a header single contains multiple values, they will be joined using a comma.

### Custom tags

It is possible to add custom tags by implementing your own version of the [HttpMetricsTagProvider](../core/src/main/java/io/stargate/core/metrics/api/HttpMetricsTagProvider.java).
When implemented and registered in the OSGi environment as a service, this provider will act as a global tag provider.
Note that you need to set the `-Dstargate.metrics.http_tag_provider.id` property to a value, in order to instruct the `core` module not to register the default `NoopHttpMetricsTagProvider`.

## All configuration options

### Listeners configuration

| Configuration option | Default | Description |
|---|---|---|
| `stargate.metrics.http_meter_listener.enabled` | `true` | If [Metering listener](#metering-listener) is enabled. |
| `stargate.metrics.http_meter_listener.ignore_http_tags_provider` | `false` | If [Custom tags](#custom-tags) provider is ignored by [Metering listener](#metering-listener). |
| `stargate.metrics.http_counter_listener.enabled` | `true` | If [Counting listener](#counting-listener) is enabled. |
| `stargate.metrics.http_counter_listener.ignore_http_tags_provider` | `false` | If [Custom tags](#custom-tags) provider is ignored by [Counting listener](#counting-listener). |


### Percentiles configuration

| Configuration option | Default | Description |
|---|---|---|
| `stargate.metrics.http_server_requests_percentiles` | Not set | Expects lists of percentiles values [0, 1] in the form `0.95,0.99` for collecting HTTP request execution times quantiles. |


### Tags configuration

| Configuration option | Default | Description |
|---|---|---|
| `stargate.metrics.http_server_requests_path_param_tags` | Not set | Expects lists of path parameters in the form `param1,param2` to extract and add as tags for each request. |
| `stargate.metrics.http_server_requests_header_tags` | Not set | Expects lists of header names in the form `header1,header2` to extract and add as tags for each request. |
