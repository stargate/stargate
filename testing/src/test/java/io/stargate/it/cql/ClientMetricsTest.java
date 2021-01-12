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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(initQueries = {"CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k))"})
public class ClientMetricsTest extends BaseOsgiIntegrationTest {
  private static final Pattern CQL_OPERATIONS_METRIC_REGEXP =
      Pattern.compile(
          "(cql_org_apache_cassandra_metrics_Client_CqlOperations_total\\s*)(\\d+.\\d+)");

  private static final String KEY = "test";

  private static String host;

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
  public void shouldIncrementCqlOperationsMetric(CqlSession session) throws IOException {
    // given
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(1);

    // when
    String body = RestUtils.get("", String.format("%s:8084/metrics/cql", host), HttpStatus.SC_OK);

    // then
    double cqlOperationsMetricValue = getCqlOperationsMetricValue(body);
    assertThat(cqlOperationsMetricValue).isGreaterThan(0);
  }

  private double getCqlOperationsMetricValue(String body) {
    return Arrays.stream(body.split("\n"))
        .filter(v -> v.contains("CqlOperations"))
        .filter(v -> CQL_OPERATIONS_METRIC_REGEXP.matcher(v).matches())
        .map(
            v -> {
              Matcher matcher = CQL_OPERATIONS_METRIC_REGEXP.matcher(v);
              if (matcher.matches()) {
                return matcher.group(2);
              }
              throw new IllegalArgumentException(
                  String.format("Value: %s does not contain the numeric value for metric", v));
            })
        .map(Double::parseDouble)
        .findAny()
        .get();
  }
}
