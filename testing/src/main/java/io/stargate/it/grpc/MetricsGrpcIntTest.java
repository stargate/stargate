/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.testing.TestingServicesActivator;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
@StargateSpec(parametersCustomizer = "buildParameters")
@Order(Integer.MAX_VALUE)
public class MetricsGrpcIntTest extends GrpcIntegrationTest {

  private static String host;

  public static void buildParameters(StargateParameters.Builder builder) {
    builder.putSystemProperties(
        TestingServicesActivator.GRPC_TAG_PROVIDER_PROPERTY,
        TestingServicesActivator.AUTHORITY_GRPC_TAG_PROVIDER);
  }

  @BeforeAll
  public static void setupHost(StargateConnectionInfo cluster) {
    host = "http://" + cluster.seedAddress();
  }

  @Test
  public void queryMetricsWithExtraTags(@TestKeyspace CqlIdentifier keyspace) {
    // given
    StargateGrpc.StargateBlockingStub stub = stubWithCallCredentials();

    // when
    QueryOuterClass.Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));

    // then
    assertThat(response).isNotNull();
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              String result =
                  RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

              // metered grpc request received lines
              List<String> requestsReceived =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server_requests_received"))
                      .collect(Collectors.toList());

              assertThat(requestsReceived)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("authority=")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\""));

              // metered grpc responses sent lines
              List<String> responsesSent =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server_responses_sent"))
                      .collect(Collectors.toList());

              assertThat(responsesSent)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("authority=")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\""));

              // metered grpc processing duration lines
              List<String> processingDuration =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server_processing_duration"))
                      .collect(Collectors.toList());

              assertThat(processingDuration)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("authority=")
                              .contains("statusCode=")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\""));
            });
  }

  @Test
  public void batchMetricsWithExtraTags(@TestKeyspace CqlIdentifier keyspace) {
    // given
    StargateGrpc.StargateBlockingStub stub = stubWithCallCredentials();

    // when
    QueryOuterClass.Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('b', 2)"))
                .setParameters(batchParameters(keyspace))
                .build());

    // then
    assertThat(response).isNotNull();
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              String result =
                  RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

              // metered grpc request received lines
              List<String> requestsReceived =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server_requests_received"))
                      .collect(Collectors.toList());

              assertThat(requestsReceived)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("authority=")
                              .contains("method=\"ExecuteBatch\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\""));

              // metered grpc responses sent lines
              List<String> responsesSent =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server_responses_sent"))
                      .collect(Collectors.toList());

              assertThat(responsesSent)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("authority=")
                              .contains("method=\"ExecuteBatch\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\""));

              // metered grpc processing duration lines
              List<String> processingDuration =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server_processing_duration"))
                      .collect(Collectors.toList());

              assertThat(processingDuration)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("authority=")
                              .contains("statusCode=")
                              .contains("method=\"ExecuteBatch\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\""));
            });
  }
}
