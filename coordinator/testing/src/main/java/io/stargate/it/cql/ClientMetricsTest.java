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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.cql;

import static io.stargate.it.MetricsTestsHelper.getMetricValue;
import static io.stargate.it.MetricsTestsHelper.getMetricValueOptional;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.TestOrder;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.testing.TestingServicesActivator;
import io.stargate.testing.metrics.FixedClientInfoTagProvider;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.http.HttpStatus;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(initQueries = {"CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k))"})
@StargateSpec(parametersCustomizer = "buildParameters")
@Order(TestOrder.LAST)
public class ClientMetricsTest extends BaseIntegrationTest {
  private static final Pattern MEMORY_HEAP_USAGE_REGEXP =
      Pattern.compile("(jvm_memory_heap_used\\s*)(\\d+.\\d+)");

  private static final Pattern MEMORY_NON_HEAP_USAGE_REGEXP =
      Pattern.compile("(jvm_memory_non_heap_used\\s*)(\\d+.\\d+)");

  private static final String KEY = "test";

  private static String host;

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.putSystemProperties(
        TestingServicesActivator.CLIENT_INFO_TAG_PROVIDER_PROPERTY,
        TestingServicesActivator.FIXED_TAG_PROVIDER);
  }

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    host = "http://" + cluster.seedAddress();
  }

  @BeforeEach
  public void cleanupData(CqlSession session) {
    session.execute("TRUNCATE test");
    session.execute("INSERT INTO test (k, v) VALUES (?, ?)", KEY, 0);
  }

  @Test
  public void shouldReportOnAndNonHeapMemoryUsed(CqlSession session) throws IOException {
    // given
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(1);

    // when
    String body = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    // then
    double heapMemoryUsed = getOnHeapMemoryUsed(body);
    assertThat(heapMemoryUsed).isGreaterThan(0);
    double nonHeapMemoryUsed = getNonHeapMemoryUsed(body);
    assertThat(nonHeapMemoryUsed).isGreaterThan(0);
  }

  @Test
  public void requestProcessedTotalWithClientInfoTags(CqlSession session) throws IOException {
    // given
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(1);

    // when
    String body = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    // then
    String requestProcessedTotal =
        String.format(
            "cql_org_apache_cassandra_metrics_Client_RequestsProcessed_total{%s=\"%s\",}",
            FixedClientInfoTagProvider.TAG_KEY, FixedClientInfoTagProvider.TAG_VALUE);
    Optional<Double> requestsProcessed = getCqlMetric(body, requestProcessedTotal);
    assertThat(requestsProcessed).hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));
  }

  @Test
  public void connectedNativeClientsWithClientInfoTags(CqlSession session) throws IOException {
    // given
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(1);

    // connected native clients are not available immediately, but when updated
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {

              // when
              String body =
                  RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

              String connectedNativeClientsTotal =
                  String.format(
                      "cql_org_apache_cassandra_metrics_Client_connectedNativeClients{%s=\"%s\",}",
                      FixedClientInfoTagProvider.TAG_KEY, FixedClientInfoTagProvider.TAG_VALUE);
              Optional<Double> connectedNativeClients =
                  getCqlMetric(body, connectedNativeClientsTotal);
              assertThat(connectedNativeClients)
                  .hasValueSatisfying(v -> assertThat(v).isGreaterThan(0d));
            });
  }

  private double getOnHeapMemoryUsed(String body) {
    return getMetricValue(body, "jvm_memory_heap_used", MEMORY_HEAP_USAGE_REGEXP);
  }

  private double getNonHeapMemoryUsed(String body) {
    return getMetricValue(body, "jvm_memory_non_heap_used", MEMORY_NON_HEAP_USAGE_REGEXP);
  }

  private Optional<Double> getCqlMetric(String body, String metric) {
    String regex =
        String.format("(%s\\s*)(\\d+.\\d+)", metric)
            .replace(",", "\\,")
            .replace("{", "\\{")
            .replace("}", "\\}");
    return getMetricValueOptional(body, metric, Pattern.compile(regex));
  }
}
