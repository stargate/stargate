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
package io.stargate.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateSpec;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@Disabled("Waiting for fixes on #232 and #250")
@StargateSpec(nodes = 3)
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    customOptions = "configureDriverMetrics",
    initQueries = "CREATE TABLE IF NOT EXISTS test (k int, cc int, v int, PRIMARY KEY(k, cc))")
public class MultipleStargateInstancesTest extends BaseIntegrationTest {

  public static void configureDriverMetrics(OptionsMap options) {
    options.put(
        TypedDriverOption.METRICS_NODE_ENABLED,
        Collections.singletonList(DefaultNodeMetric.CQL_MESSAGES.getPath()));
  }

  private static int nodeCount;

  @BeforeAll
  public static void beforeAll(StargateEnvironmentInfo stargate) {
    nodeCount = stargate.nodes().size();
  }

  @Test
  public void shouldConnectToMultipleStargateNodes(CqlSession session) {
    List<Row> all = session.execute("SELECT * FROM system.peers").all();
    // system.peers should have N records (all stargate nodes - 1)
    assertThat(all.size()).isEqualTo(nodeCount - 1);
  }

  @Test
  public void shouldDistributeTrafficUniformly(CqlSession session) {
    // given
    int numberOfRequestPerNode = 100;
    int totalNumberOfRequests = numberOfRequestPerNode * nodeCount;
    // difference tolerance - every node should have numberOfRequestPerNode +- tolerance
    long tolerance = 5;

    // when
    for (int i = 0; i < totalNumberOfRequests; i++) {
      session.execute("INSERT INTO test (k, cc, v) VALUES (1, ?, ?)", i, i);
    }

    // then
    Collection<Node> nodes = session.getMetadata().getNodes().values();
    assertThat(nodes).hasSize(nodeCount);
    for (Node n : nodes) {
      long cqlMessages =
          session
              .getMetrics()
              .flatMap(metrics -> metrics.<Timer>getNodeMetric(n, DefaultNodeMetric.CQL_MESSAGES))
              .orElseThrow(this::failOnMissingMetric)
              .getCount();
      assertThat(cqlMessages)
          .isBetween(numberOfRequestPerNode - tolerance, numberOfRequestPerNode + tolerance);
    }
  }

  private AssertionError failOnMissingMetric() {
    return new AssertionError(
        String.format(
            "Expected to find metric %s since it was enabled in the test",
            DefaultNodeMetric.CQL_MESSAGES.getPath()));
  }
}
