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
package io.stargate.sgv2.graphql.integration.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import io.stargate.sgv2.graphql.integration.util.MetricsTestsHelper;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MetricsIntegrationTest extends CqlFirstIntegrationTest {

  private static final Pattern GRAPHQL_OPERATIONS_METRIC_REGEXP =
      Pattern.compile("(http_server_requests_seconds_count.*)\\s(\\d+.\\d+)");

  @BeforeAll
  public void createSchema() {
    session.execute("CREATE TABLE \"Foo\"(k int PRIMARY KEY, v int)");
  }

  @Test
  public void shouldIncrementMetricWhenExecutingGraphQlQuery() {

    // Execute a first query before getting the count, because it is initialized lazily.
    client.executeDmlQuery(
        keyspaceId.asInternal(), "mutation { insertFoo(value: { k: 1, v: 1 } ) { applied} }");

    int countBefore = getQueryCount();
    client.executeDmlQuery(keyspaceId.asInternal(), "{ Foo(value: { k: 1 }) { values { v } } }");
    client.executeDmlQuery(keyspaceId.asInternal(), "{ Foo(value: { k: 1 }) { values { v } } }");

    await()
        .untilAsserted(
            () -> {
              int countAfter = getQueryCount();
              // Don't require an exact match in case other tests are running concurrently
              assertThat(countAfter - countBefore).isGreaterThanOrEqualTo(2);
            });
  }

  private int getQueryCount() {
    String body = client.getMetrics();
    Collector<Double, ?, Double> aggregator = Collectors.summingDouble(Double::doubleValue);
    Double value =
        MetricsTestsHelper.getMetricValue(
            body, "graphqlapi", GRAPHQL_OPERATIONS_METRIC_REGEXP, aggregator);
    return value.intValue();
  }
}
