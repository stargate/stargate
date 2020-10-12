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
package io.stargate.it.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import java.lang.reflect.AnnotatedElement;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit 5 extension for tests that need a backend database cluster managed by {@code ccm}. */
public class ExternalStorage
    implements BeforeAllCallback, AfterAllCallback, ParameterResolver, BeforeTestExecutionCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalStorage.class);

  private static final String CCM_VERSION = "ccm.version";
  private static final boolean EXTERNAL_BACKEND =
      Boolean.getBoolean("stargate.test.backend.use.external");
  private static final String DATACENTER = System.getProperty("stargate.test.backend.dc", "dc1");
  private static final String CLUSTER_NAME =
      System.getProperty("stargate.test.backend.cluster_name", "Test_Cluster");

  private static final AtomicBoolean executing = new AtomicBoolean();

  private static final int clusterNodes = Integer.getInteger("stargate.test.backend.nodes", 1);

  private static Cluster cluster;

  static {
    String version = System.getProperty(CCM_VERSION, "3.11.8");
    System.setProperty(CCM_VERSION, version);
  }

  private static Cluster cluster() {
    if (cluster == null) {
      throw new IllegalStateException("Cluster not started");
    }

    return cluster;
  }

  private static ClusterSpec defaultSpec() {
    return DefaultClusterSpecHolder.class.getAnnotation(ClusterSpec.class);
  }

  private static synchronized void start(ClusterSpec spec, ExtensionContext context) {
    // Make sure all shared cluster use the same (default) spec.
    if (spec.scope() == ClusterScope.SHARED) {
      assertEquals(defaultSpec(), spec);
    }

    String initSite = context.getDisplayName();

    Cluster c = ExternalStorage.cluster;
    if (c != null) {
      if (c.spec.scope() == ClusterScope.SHARED && spec.scope() == ClusterScope.SHARED) {
        return;
      }

      LOG.warn(
          "Stopping cluster started for '{}' due to conflicting specs:"
              + " {} is not compatible with {} (which is requested by '{}')",
          c.initSite,
          c.spec,
          spec,
          initSite);
      c.stop();
    }

    c = new Cluster(spec, initSite);
    c.start();
    ExternalStorage.cluster = c;
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    if (!executing.compareAndSet(false, true)) {
      throw new IllegalStateException(
          String.format(
              "Concurrent execution with %s is not supported", getClass().getSimpleName()));
    }

    AnnotatedElement test =
        context
            .getElement()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Test class or method is not available in current context"));

    ClusterSpec spec = test.getAnnotation(ClusterSpec.class);
    if (spec == null) {
      spec = defaultSpec();
    }

    assertNotNull(spec, "ClusterSpec is not available");
    start(spec, context);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    Cluster c = cluster;
    if (c != null) {
      ClusterScope scope = c.spec.scope();
      if (scope != ClusterScope.SHARED) {
        c.stop();
        cluster = null;
      }
    }

    executing.set(false);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(ClusterConnectionInfo.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return cluster();
  }

  private String getFullTestPath(ExtensionContext context) {
    StringBuilder sb = new StringBuilder();
    while (context != null) {
      sb.insert(0, context.getDisplayName());
      sb.insert(0, '/');
      context = context.getParent().orElse(null);
    }
    return sb.toString();
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    LOG.info(
        "About to run {} with storage cluster version {}",
        getFullTestPath(context),
        cluster().clusterVersion());
  }

  @ClusterSpec
  private static class DefaultClusterSpecHolder {}

  private static class Cluster implements ClusterConnectionInfo {

    private final ClusterSpec spec;
    private final String initSite;
    private final CcmBridge ccm;
    private final AtomicBoolean removed = new AtomicBoolean();

    private Cluster(ClusterSpec spec, String displayName) {
      this.spec = spec;
      this.initSite = displayName;
      this.ccm =
          CcmBridge.builder()
              .withCreateOption("-R")
              .withCassandraConfiguration("cluster_name", CLUSTER_NAME)
              .withNodes(clusterNodes)
              .build();
    }

    public void start() {
      if (!EXTERNAL_BACKEND) {
        ccm.create();
        ccm.start();

        if (spec.scope() == ClusterScope.SHARED) {
          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread("ccm-shutdown-hook:" + initSite) {
                    @Override
                    public void run() {
                      Cluster.this.stop();
                    }
                  });
        }

        LOG.info("Storage cluster requested by {} has been started.", initSite);
      }
    }

    public void stop() {
      if (!EXTERNAL_BACKEND) {
        try {
          if (removed.compareAndSet(false, true)) {
            ccm.remove();
            LOG.info(
                "Storage cluster (version {}) that was requested by {} has been removed.",
                clusterVersion(),
                initSite);
          }
        } catch (Exception e) {
          // This should not affect test result validity, hence logging as WARN
          LOG.warn("Exception during CCM cluster shutdown: {}", e.toString(), e);
        }
      }
    }

    @Override
    public String seedAddress() {
      return "127.0.0.1";
    }

    @Override
    public int storagePort() {
      return 7000;
    }

    @Override
    public int cqlPort() {
      return 9042;
    }

    @Override
    public String clusterName() {
      return CLUSTER_NAME;
    }

    @Override
    public String clusterVersion() {
      Version version = ccm.getDseVersion().orElse(ccm.getCassandraVersion());
      return String.format("%d.%d", version.getMajor(), version.getMinor());
    }

    @Override
    public boolean isDse() {
      return ccm.getDseVersion().isPresent();
    }

    @Override
    public String datacenter() {
      return DATACENTER;
    }

    @Override
    public String rack() {
      return "rack1";
    }
  }
}
