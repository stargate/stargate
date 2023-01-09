# All Freebies with Quarkus
Showcases Quarkus benefits on a Stargate Docs API V2 project.

Prerequisites to follow this tutorial:

* Start Stargate coordinator (with C* node if not in dev mode)
  ```bash
  docker run \
   --rm \
   -e CLUSTER_NAME=quarkus-freebies \
   -e CLUSTER_VERSION=4.0 \
   -e ENABLE_AUTH=true \
   -e DEVELOPER_MODE=true \
   -p 8081:8081 \
   -p 8091:8091 \
   stargateio/coordinator-4_0:v2
  ```
* Get authentication token if you want to test Swagger
  ```bash
  curl -L -X POST "http://localhost:8081/v1/auth" \
   -H 'Content-Type: application/json' \
   --data-raw '{
    "username": "cassandra",
    "password": "cassandra"
   }' | jq -r '.authToken'
  ```
* Run `./mvnw clean install -DskipTests` on the `/apis` root

## Dev mode
Quarkus comes with a handy development mode activated with `quarkus:dev`: 

```shell
../mvnw quarkus:dev
```

The dev mode brings:
* [Development UI](http://localhost:8180)
* Resumable tests - `r` to run all tests, `f` to run failed tests
* Live-reloads - make any update to the source code and it's applied automatically
* Easy debugging - hit `Run -> Attach to process` in IntelliJ


## Configuration 
*Uses [quarkus-config-yaml](https://quarkus.io/guides/config-yaml) extension.*

Easily integrate into Quarkus configuration possibilities that enables each config property to be specified using system properties, env variables, application profile files, etc.

* Specified using `@ConfigMapping` and interfaces -> [QueriesConfig](../sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/config/QueriesConfig.java)
* Applied using any of the supported methods:
   ```shell
   ../mvnw quarkus:dev -Dstargate.queries.consistency.reads=ONE
   ```

## gRPC support
*Uses [quarkus-grpc](https://quarkus.io/guides/config-yaml) extension.*

Easily generate stubs for consuming a gRPC service:

* Define in [pom.xml](../sgv2-quarkus-common/pom.xml) what protos should be used for generation:
  ```xml
  <quarkus.generate-code.grpc.scan-for-imports>com.google.protobuf:protobuf-java,com.google.api.grpc:proto-google-common-protos</quarkus.generate-code.grpc.scan-for-imports>
  <quarkus.generate-code.grpc.scan-for-proto>io.stargate.grpc:bridge-proto</quarkus.generate-code.grpc.scan-for-proto>
  ```
* Different stubs available out of the box:
   * Reactive using Mutiny API (default) - [StargateBridge](../sgv2-quarkus-common/target/generated-sources/grpc/io/stargate/bridge/proto/StargateBridge.java)
   * Blocking stub, Future sub, etc - [StargateBridgeGrpc](../sgv2-quarkus-common/target/generated-sources/grpc/io/stargate/bridge/proto/StargateBridgeGrpc.java)

## OpenAPI & Swagger
*Uses [quarkus-smallrye-openapi](https://quarkus.io/guides/openapi-swaggerui) extension.*

* Fully configurable OpenAPI definitions from source code annotations, available as YAML at http://localhost:8180/api/docs/openapi.
* Includes [Swagger UI v3](http://localhost:8180/swagger-ui)

## Observability
*Uses [quarkus-micrometer-registry-prometheus](https://quarkus.io/guides/micrometer) and [quarkus-opentelemetry](https://quarkus.io/guides/opentelemetry) extension.*

* Auto-integrated metrics with Micrometer:
   * Out-of-the-box metric for JVM, server and client HTTP & gRPC metrics, caches, etc
   * Injectable `MeterRegistry`
   * Multiple registries supported
* Support for tracing using OpenTelemetry
   * Out-of-the-box traces for JVM boundaries (HTTP and gRPC both supported)
   * Custom spans using `@WithSpan`

Below bash commands start the `jaegertracing/opentelemetry-all-in-one` docker container and the open telemetry collection enabled in the dev mode:

* Metrics available at http://localhost:8180/metrics
* Traces available at http://localhost:16686

```bash
docker run \
  -d \
  --rm \
  -e SPAN_STORAGE_TYPE=memory \
  -p 55680:55680 \
  -p 16686:16686 \
  jaegertracing/opentelemetry-all-in-one
  
../mvnw quarkus:dev -Dstargate.queries.consistency.reads=ONE -Dquarkus.opentelemetry.tracer.exporter.otlp.enabled=true -Dquarkus.opentelemetry.tracer.exporter.otlp.endpoint=http://localhost:55680
```

## Building images
*Uses [quarkus-container-image-docker](https://quarkus.io/guides/container-image) extension.*

Integrated Docker image building.

* Build docker image
  ```bash
  ../mvnw clean package -Dquarkus.container-image.build=true -DskipTests
  ```
* Build docker image with native executable
  ```bash
  ../mvnw clean package -Pnative -Dquarkus.native.container-build=true -Dquarkus.container-image.build=true -DskipTests
  ```

> *NOTE: Quarkus supports Jib, Docker, S2I and Buildpack. In addition, non-container based builds are available as well (fat jar, native executable).*

## Integration tests

Quarkus has a fantastic support for the integration tests with `@QuarkusIntegrationTest`.
These are more end-to-end tests, and should not be confused with `@QuarkusTest` (which is used for injectable int-tests similar to the `@SpringBootTest`).
The biggest benefit of these tests is that the test code is independent from the context and dependency injections.
In addition, it's easy to spin up necessary testing dependencies using Testcontainers (in our case, Stargate coordinator and C* node).

Example can be seen in [CollectionsResourceIntegrationTest](../sgv2-docsapi/src/test/java/io/stargate/sgv2/docsapi/api/v2/namespaces/collections/CollectionsResourceIntegrationTest.java).

Thus it's possible:

* To run integration tests against the build artifact of the project, for example the docker container (stop the Stargate coordinator before running this test):

  ```shell
  ../mvnw verify -Dquarkus.container-image.build=true -DskipUnitTests
  ```
* To run integration tests against already running instance of the application:

  ```shell
  ../mvnw verify -Dquarkus.http.test-host=1.2.3.4 -Dquarkus.http.test-port=4321 -DskipUnitTests
  ```

## And much more

* Health endpoints automatically exposed using `quarkus-smallrye-health` extension
* Validation support with `hibernate-validator` and `javax.validation` annotations
* Fault tolerance with annotations
* Caching with configuration support and metrics 
