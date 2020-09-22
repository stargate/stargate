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

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tests that need a backend database cluster managed by {@code ccm}.
 *
 * <p>Note: the cluster is created on class initialization and is destroyed at JVM exit.
 */
public class BaseStorageIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseStorageIntegrationTest.class);

  public static final String CLUSTER_NAME = "Test_Stargate_Cluster";

  private static final String CCM_VERSION = "ccm.version";
  private static final boolean EXTERNAL_BACKEND =
      Boolean.getBoolean("stargate.test.backend.use.external");

  private static final int clusterNodes = Integer.getInteger("stargate.test.backend.nodes", 1);
  private static final CcmBridge ccm;

  static {
    String version = System.getProperty(CCM_VERSION, "3.11.8");
    System.setProperty(CCM_VERSION, version);

    ccm =
        CcmBridge.builder()
            .withCassandraConfiguration("cluster_name", CLUSTER_NAME)
            .withNodes(clusterNodes)
            .build();

    if (!EXTERNAL_BACKEND) {
      ccm.create();
      ccm.start();

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread("ccm-shutdown-hook") {
                @Override
                public void run() {
                  try {
                    ccm.remove();
                  } catch (Exception e) {
                    // This should not affect test result validity, hence logging as WARN
                    LOG.warn("Exception during CCM cluster shutdown: {}", e.toString(), e);
                  }
                }
              });
    }
  }

  protected static String seedAddress() {
    return "127.0.0.1";
  }

  protected static int storagePort() {
    return 7000;
  }

  protected static String clusterVersion() {
    Version version = ccm.getDseVersion().orElse(ccm.getCassandraVersion());
    return String.format("%d.%d", version.getMajor(), version.getMinor());
  }

  protected static boolean isDse() {
    return ccm.getDseVersion().isPresent();
  }

  protected static String datacenter() {
    return isDse() ? "dc1" : "datacenter1";
  }

  protected static String rack() {
    return "rack1";
  }
}
