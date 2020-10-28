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

import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.ClusterSpec;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateSpec;
import io.stargate.it.storage.UseStargateContainer;
import java.io.File;
import java.net.URL;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class manages starting Stargate OSGi containers. */
@UseStargateContainer
@ClusterSpec(shared = true)
@StargateSpec(shared = true)
public class BaseOsgiIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(BaseOsgiIntegrationTest.class);

  protected ClusterConnectionInfo backend;

  static {
    ClassLoader classLoader = BaseOsgiIntegrationTest.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }
  }

  @BeforeEach
  public void init(ClusterConnectionInfo backend) {
    this.backend = backend;
  }

  /**
   * Waits until a CQL session sees all the Stargate nodes in its metadata. The CI environment is
   * slow and it may take a while until system.peers is up to date.
   */
  protected void awaitAllNodes(CqlSession session, StargateEnvironmentInfo stargate) {
    int expectedNodeCount = stargate.nodes().size();
    await()
        .atMost(Duration.ofMinutes(10))
        .pollInterval(Duration.ofSeconds(10))
        .until(
            () -> {
              boolean connected = session.getMetadata().getNodes().size() == expectedNodeCount;
              LOG.debug(
                  "Expected: {}, in driver metadata: {}, in system tables: {}",
                  expectedNodeCount,
                  session.getMetadata().getNodes().size(),
                  session.execute("SELECT * FROM system.peers").all().size() + 1);

              return connected;
            });
  }
}
