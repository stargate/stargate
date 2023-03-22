package io.stargate.it.grpc;

import static io.stargate.it.MetricsTestsHelper.getMetricValueOptional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.Values;
import io.stargate.it.TestOrder;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
@Order()
public class AuthApiServerMetricsTest extends GrpcIntegrationTest {
  // run this at the begining when there are not a lot of metrics (faster)
  private static final int ORDER = TestOrder.FIRST + 1;

  private static String host;

  @BeforeAll
  public static void init(StargateConnectionInfo cluster) {
    host = "http://" + cluster.seedAddress();
  }

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE test");
  }

  @Test
  public void queryMetrics(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));
    assertThat(response).isNotNull();

    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    assertThat(response.hasResultSet()).isTrue();

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              String result =
                  RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

              List<String> lines =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server"))
                      .collect(Collectors.toList());

              assertThat(lines)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_count")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"OK\""))
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_sum")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"OK\""))
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_max")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"OK\""));

              String responsesTotal =
                  "grpc_server_responses_sent_messages_total{method=\"ExecuteQuery\",methodType=\"UNARY\",service=\"stargate.Stargate\",}";
              String requestsTotal =
                  "grpc_server_requests_received_messages_total{method=\"ExecuteQuery\",methodType=\"UNARY\",service=\"stargate.Stargate\",}";

              assertThat(getGrpcMetric(result, responsesTotal))
                  .hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));

              assertThat(getGrpcMetric(result, requestsTotal))
                  .hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));
            });
  }

  @Test
  public void batchMetrics(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .setParameters(batchParameters(keyspace))
                .build());
    assertThat(response).isNotNull();

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              String result =
                  RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

              List<String> lines =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server"))
                      .collect(Collectors.toList());

              System.out.println(lines);
              assertThat(lines)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_count")
                              .contains("method=\"ExecuteBatch\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"OK\""))
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_sum")
                              .contains("method=\"ExecuteBatch\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"OK\""))
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_max")
                              .contains("method=\"ExecuteBatch\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"OK\""));

              String responsesTotal =
                  "grpc_server_responses_sent_messages_total{method=\"ExecuteBatch\",methodType=\"UNARY\",service=\"stargate.Stargate\",}";
              String requestsTotal =
                  "grpc_server_requests_received_messages_total{method=\"ExecuteBatch\",methodType=\"UNARY\",service=\"stargate.Stargate\",}";

              assertThat(getGrpcMetric(result, responsesTotal))
                  .hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));

              assertThat(getGrpcMetric(result, requestsTotal))
                  .hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));
            });
  }

  @Test
  public void unauthenticatedMetrics() {
    assertThatThrownBy(
            () ->
                stubWithCallCredentials("not-a-token-that-exists")
                    .executeQuery(Query.newBuilder().setCql("SELECT * FROM system.local").build()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED")
        .hasMessageContaining("Invalid token");

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              String result =
                  RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

              List<String> lines =
                  Arrays.stream(result.split(System.getProperty("line.separator")))
                      .filter(line -> line.startsWith("grpc_server"))
                      .collect(Collectors.toList());

              assertThat(lines)
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_count")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"UNAUTHENTICATED\""))
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_sum")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"UNAUTHENTICATED\""))
                  .anySatisfy(
                      metric ->
                          assertThat(metric)
                              .contains("grpc_server_processing_duration_seconds_max")
                              .contains("method=\"ExecuteQuery\"")
                              .contains("methodType=\"UNARY\"")
                              .contains("service=\"stargate.Stargate\"")
                              .contains("statusCode=\"UNAUTHENTICATED\""));

              String durationCount =
                  "grpc_server_processing_duration_seconds_count{method=\"ExecuteQuery\",methodType=\"UNARY\",service=\"stargate.Stargate\",statusCode=\"UNAUTHENTICATED\",}";

              assertThat(getGrpcMetric(result, durationCount))
                  .hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));
            });
  }

  private Optional<Double> getGrpcMetric(String body, String metric) {
    String regex =
        String.format("(%s\\s*)(\\d+.\\d+)", metric)
            .replace(",", "\\,")
            .replace("{", "\\{")
            .replace("}", "\\}");
    return getMetricValueOptional(body, metric, Pattern.compile(regex));
  }
}
