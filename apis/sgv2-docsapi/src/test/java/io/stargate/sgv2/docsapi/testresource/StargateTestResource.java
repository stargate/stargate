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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.testresource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * Quarkus test resource that starts Cassandra/DSE and Stargate Coordinator using test containers.
 *
 * <p>Should be used in the integration tests, that should be explicitly annotated with {@link
 * IntegrationTestProfile} class: <code>@TestProfile(IntegrationTestProfile.class)</code>
 *
 * <p>When run from IDE, by default it uses container versions specified in {@link Defaults}. If you
 * wish to run locally with different Cassandra version or the DSE, please set up following system
 * properties:
 *
 * <ol>
 *   <li><code>testing.containers.cassandra-image</code>
 *   <li><code>testing.containers.stargate-image</code>
 *   <li><code>testing.containers.cluster-version</code>
 *   <li><code>testing.containers.cluster-dse</code>
 * </ol>
 *
 * <p>Note that this resource fetches the auth token and sets it via the properties, using the
 * {@link io.stargate.sgv2.docsapi.api.common.token.impl.FixedTokenResolver}.
 */
public class StargateTestResource
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  /**
   * Set of defaults for the integration tests, usually used when running from IDE.
   *
   * <p><b>IMPORTANT:</b> If changing defaults please update the default properties in the pom.xml
   * for the cassandra-40 profile.
   */
  interface Defaults {

    String CASSANDRA_IMAGE = "cassandra";
    String CASSANDRA_IMAGE_TAG = "4.0.4";

    String STARGATE_IMAGE = "stargateio/coordinator-4_0";
    String STARGATE_IMAGE_TAG = "latest";

    String CLUSTER_NAME = "int-test-cluster";
    String CLUSTER_VERSION = "4.0";

    String CLUSTER_DSE = null;
  }

  private static final Logger LOG = LoggerFactory.getLogger(StargateTestResource.class);

  private Optional<String> containerNetworkId;

  private Network network;

  private GenericContainer<?> cassandraContainer;

  private GenericContainer<?> stargateContainer;

  /**
   * {@inheritDoc}
   *
   * <p><i>Note: container network ID will be present if resource is used
   * with @QuarkusIntegrationTest</i>
   */
  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    // TODO make reusable after https://github.com/testcontainers/testcontainers-java/pull/4777
    // boolean reuse = containerNetworkId.isEmpty();
    boolean reuse = false;

    cassandraContainer =
        new GenericContainer<>(getCassandraImage())
            .withEnv("HEAP_NEWSIZE", "512M")
            .withEnv("MAX_HEAP_SIZE", "2048M")
            .withEnv("CASSANDRA_CGROUP_MEMORY_LIMIT", "true")
            .withEnv(
                "JVM_EXTRA_OPTS",
                "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false -Dcassandra.initial_token=1")
            .withNetworkAliases("cassandra")
            .withExposedPorts(7000)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("cassandra-docker")))
            .waitingFor(Wait.forLogMessage(".*Created default superuser role.*\\n", 1))
            .withStartupTimeout(Duration.ofMinutes(2))
            .withReuse(reuse);
    // note that cluster name props differ in case of DSE
    if (isDse()) {
      cassandraContainer.withEnv("CLUSTER_NAME", getClusterName()).withEnv("DS_LICENSE", "accept");
    } else {
      cassandraContainer.withEnv("CASSANDRA_CLUSTER_NAME", getClusterName());
    }
    // resolve the network based on the mode
    if (containerNetworkId.isPresent()) {
      cassandraContainer.withNetworkMode(containerNetworkId.get());
    } else {
      cassandraContainer.withNetwork(network());
    }
    cassandraContainer.start();

    stargateContainer =
        new GenericContainer<>(getStargateImage())
            .withEnv("JAVA_OPTS", "-Xmx1G")
            .withEnv("CLUSTER_NAME", getClusterName())
            .withEnv("CLUSTER_VERSION", getClusterVersion())
            .withEnv("SEED", "cassandra")
            .withEnv("SIMPLE_SNITCH", "true")
            .withEnv("ENABLE_AUTH", "true")
            .withNetworkAliases("coordinator")
            .withExposedPorts(8091, 8081, 8084)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("coordinator-docker")))
            .waitingFor(Wait.forHttp("/checker/readiness").forPort(8084).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2))
            .withReuse(reuse);
    // enable DSE if needed
    if (isDse()) {
      stargateContainer.withEnv("DSE", "1");
    }
    // resolve the network based on the mode
    if (containerNetworkId.isPresent()) {
      stargateContainer.withNetworkMode(containerNetworkId.get());
    } else {
      stargateContainer.withNetwork(network());
    }
    stargateContainer.start();

    Integer authPort = stargateContainer.getMappedPort(8081);
    Integer bridgePort = stargateContainer.getMappedPort(8091);

    // get auth token
    String token = getAuthToken("localhost", authPort);
    LOG.info("Using auth token %s for integration tests.".formatted(token));

    // TODO if no end-to-end tests, inject the token instead of fixing it
    // return a map containing the configuration the application needs to use the service
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.put("stargate.auth.token-resolver.type", "fixed");
    propsBuilder.put("stargate.auth.token-resolver.fixed.token", token);

    if (containerNetworkId.isPresent()) {
      propsBuilder.put("quarkus.grpc.clients.bridge.host", "coordinator");
    } else {
      propsBuilder.put("quarkus.grpc.clients.bridge.port", String.valueOf(bridgePort));
    }

    ImmutableMap<String, String> props = propsBuilder.build();
    LOG.info("Using props map for the integration tests: %s".formatted(props));
    return props;
  }

  @Override
  public void stop() {
    if (null != cassandraContainer && !cassandraContainer.isShouldBeReused()) {
      cassandraContainer.stop();
    }
    if (null != stargateContainer && !stargateContainer.isShouldBeReused()) {
      stargateContainer.stop();
    }
  }

  private Network network() {
    if (null == network) {
      network = Network.newNetwork();
    }
    return network;
  }

  private String getCassandraImage() {
    String image = System.getProperty("testing.containers.cassandra-image");
    if (null == image) {
      return Defaults.CASSANDRA_IMAGE + ":" + Defaults.CASSANDRA_IMAGE_TAG;
    } else {
      return image;
    }
  }

  private String getStargateImage() {
    String image = System.getProperty("testing.containers.stargate-image");
    if (null == image) {
      return Defaults.STARGATE_IMAGE + ":" + Defaults.STARGATE_IMAGE_TAG;
    } else {
      return image;
    }
  }

  private String getClusterName() {
    return System.getProperty("testing.containers.cluster-name", Defaults.CLUSTER_NAME);
  }

  private String getClusterVersion() {
    return System.getProperty("testing.containers.cluster-version", Defaults.CLUSTER_VERSION);
  }

  private boolean isDse() {
    String dse = System.getProperty("testing.containers.cluster-dse", Defaults.CLUSTER_DSE);
    return "true".equals(dse);
  }

  private String getAuthToken(String host, int authPort) {
    try {
      // make call to the coordinator auth api
      String json =
          """
              {
                "username":"cassandra",
                "password":"cassandra"
              }
              """;
      URI authUri = new URI("http://%s:%d/v1/auth".formatted(host, authPort));
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(authUri)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(json))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

      // map to response and read token
      ObjectMapper objectMapper = new ObjectMapper();
      AuthResponse authResponse = objectMapper.readValue(response.body(), AuthResponse.class);
      return authResponse.authToken;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get Cassandra token for integration tests.", e);
    }
  }

  record AuthResponse(String authToken) {}
}
