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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@NotThreadSafe
public class MultipleStargateInstancesTest extends BaseOsgiIntegrationTest {

  @Rule public TestName name = new TestName();

  private String table;

  private String keyspace;

  private CqlSession session;

  @Before
  public void setup() {
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, false)
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                DcInferringLoadBalancingPolicy.class.getName())
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
            .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(5))
            .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(1))
            .build();

    CqlSessionBuilder cqlSessionBuilder = CqlSession.builder().withConfigLoader(loader);
    for (String host : getStargateHosts()) {
      cqlSessionBuilder.addContactPoint(new InetSocketAddress(host, 9043));
    }
    session = cqlSessionBuilder.build();

    String testName = name.getMethodName();
    if (testName.contains("[")) {
      testName = testName.substring(0, testName.indexOf("["));
    }
    keyspace = "ks_" + testName;
    table = testName;
  }

  @Test
  public void shouldConnectToMultipleStargateNodes() {
    List<Row> all = session.execute("SELECT * FROM system.peers").all();
    // system.peers should have 2 records (all stargate nodes - 1)
    assertThat(all.size()).isEqualTo(2);
  }
}
