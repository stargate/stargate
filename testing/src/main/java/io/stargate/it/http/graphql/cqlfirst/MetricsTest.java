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
package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.MetricsTestsHelper;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(initQueries = {"CREATE TABLE \"Foo\"(k int PRIMARY KEY, v int)"})
public class MetricsTest extends BaseIntegrationTest {

  private static final Pattern GRAPHQL_OPERATIONS_METRIC_REGEXP =
      Pattern.compile(
          "(graphqlapi_io_dropwizard_jetty_MutableServletContextHandler_dispatches_count\\s*)(\\d+.\\d+)");

  private static String HOST;
  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    HOST = cluster.seedAddress();
    CLIENT = new CqlFirstClient(HOST, RestUtils.getAuthToken(HOST));
  }

  @Test
  public void shouldIncrementMetricWhenExecutingGraphQlQuery(
      @TestKeyspace CqlIdentifier keyspaceId) {
    int countBefore = getQueryCount();
    CLIENT.executeDmlQuery(keyspaceId, "mutation { insertFoo(value: { k: 1, v: 1 } ) { applied} }");
    CLIENT.executeDmlQuery(keyspaceId, "{ Foo(value: { k: 1 }) { values { v } } }");
    int countAfter = getQueryCount();

    // Don't require an exact match in case other tests are running concurrently
    assertThat(countAfter - countBefore).isGreaterThanOrEqualTo(2);
  }

  private int getQueryCount() {
    try {
      String body =
          RestUtils.get("", String.format("http://%s:8084/metrics", HOST), HttpStatus.SC_OK);
      return (int)
          MetricsTestsHelper.getMetricValue(body, "graphqlapi", GRAPHQL_OPERATIONS_METRIC_REGEXP);
    } catch (IOException e) {
      fail("Unexpected error while querying metrics", e);
      return -1; // never reached
    }
  }
}
