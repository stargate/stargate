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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateSpec;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@NotThreadSafe
@Disabled("Waiting for fixes on #232 and #250")
@StargateSpec(nodes = 3)
public class MultipleStargateInstancesTest extends BaseOsgiIntegrationTest {

  private String table;

  private String keyspace;

  private CqlSession session;

  private int runningStargateNodes;

  @BeforeEach
  public void setup(TestInfo testInfo, StargateEnvironmentInfo stargate) {
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, false)
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                DcInferringLoadBalancingPolicy.class.getName())
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(180))
            .withDuration(
                DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(180))
            .withDuration(
                DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(180))
            .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(180))
            .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5))
            .withStringList(
                DefaultDriverOption.METRICS_NODE_ENABLED,
                Collections.singletonList(DefaultNodeMetric.CQL_MESSAGES.getPath()))
            .build();

    CqlSessionBuilder cqlSessionBuilder = CqlSession.builder().withConfigLoader(loader);

    for (StargateConnectionInfo node : stargate.nodes()) {
      cqlSessionBuilder.addContactPoint(new InetSocketAddress(node.seedAddress(), node.cqlPort()));
    }
    session = cqlSessionBuilder.build();

    Optional<String> name = testInfo.getTestMethod().map(Method::getName);
    assertThat(name).isPresent();
    String testName = name.get();
    keyspace = "ks_" + testName;
    table = testName;
    runningStargateNodes = stargate.nodes().size();
  }

  @AfterEach
  public void teardown() {
    session.close();
  }

  @Test
  public void shouldConnectToMultipleStargateNodes() {
    List<Row> all = session.execute("SELECT * FROM system.peers").all();
    // system.peers should have N records (all stargate nodes - 1)
    assertThat(all.size()).isEqualTo(runningStargateNodes - 1);
  }

  @Test
  public void shouldDistributeTrafficUniformly() {
    // given
    createKeyspaceAndTable();
    int numberOfRequestPerNode = 100;
    int totalNumberOfRequests = numberOfRequestPerNode * runningStargateNodes;
    // difference tolerance - every node should have numberOfRequestPerNode +- tolerance
    long tolerance = 5;

    // when
    for (int i = 0; i < totalNumberOfRequests; i++) {
      session.execute(
          SimpleStatement.newInstance(
              String.format(
                  "INSERT INTO \"%s\".\"%s\" (k, cc, v) VALUES (1, ?, ?)", keyspace, table),
              i,
              i));
    }

    // then
    Collection<Node> nodes = session.getMetadata().getNodes().values();
    assertThat(nodes).hasSize(runningStargateNodes);
    for (Node n : nodes) {
      long cqlMessages =
          ((Timer)
                  session.getMetrics().get().getNodeMetric(n, DefaultNodeMetric.CQL_MESSAGES).get())
              .getCount();
      assertThat(cqlMessages)
          .isBetween(numberOfRequestPerNode - tolerance, numberOfRequestPerNode + tolerance);
    }
  }

  private void createKeyspace() {
    session.execute(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
            keyspace));
  }

  private void createKeyspaceAndTable() {
    createKeyspace();

    session.execute(
        SimpleStatement.newInstance(
            String.format(
                "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (k int, cc int, v int, PRIMARY KEY(k, cc))",
                keyspace, table)));
  }
}
