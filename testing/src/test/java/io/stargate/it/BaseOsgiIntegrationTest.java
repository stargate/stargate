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

import static io.stargate.it.storage.ClusterScope.SHARED;

import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.ClusterSpec;
import io.stargate.it.storage.ExternalStorage;
import io.stargate.starter.Starter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class manages starting Stargate OSGi containers. */
@ExtendWith(ExternalStorage.class)
@ClusterSpec(scope = SHARED)
public class BaseOsgiIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(BaseOsgiIntegrationTest.class);

  public boolean enableAuth;

  protected final ClusterConnectionInfo backend;

  public static List<Starter> stargateStarters = new ArrayList<>();
  private static List<String> stargateHosts = new ArrayList<>();
  public static final Integer numberOfStargateNodes = Integer.getInteger("stargate.test.nodes", 3);

  static {
    for (int i = 1; i <= numberOfStargateNodes; i++) {
      int portSuffix = 10 + i;
      stargateHosts.add("127.0.0." + portSuffix);
    }
  }

  public BaseOsgiIntegrationTest(ClusterConnectionInfo backend) {
    this.backend = backend;
  }

  public static String getStargateHost() {
    return getStargateHost(0);
  }

  public static String getStargateHost(int stargateInstanceNumber) {
    return stargateHosts.get(stargateInstanceNumber);
  }

  public static List<String> getStargateHosts() {
    return stargateHosts;
  }

  public static List<InetAddress> getStargateInetSocketAddresses() {
    return stargateHosts.stream()
        .map(
            h -> {
              try {
                return InetAddress.getByName(h);
              } catch (UnknownHostException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  @BeforeEach
  public void startOsgi() {
    if (stargateStarters.isEmpty()) {
      logger.info("Starting: {} stargate nodes", numberOfStargateNodes);
      for (int i = 0; i < numberOfStargateNodes; i++) {
        try {
          startStargateInstance(backend.seedAddress(), backend.storagePort(), i);
        } catch (Exception ex) {
          logger.error(
              "Exception when starting stargate node nr: "
                  + i
                  + ". The stargate node will not be started.",
              ex);

          // If the container fails to start, there's no point running individual test cases.
          throw new IllegalStateException(ex);
        }
      }
    }
  }

  private void startStargateInstance(String seedHost, Integer seedPort, int stargateNodeNumber)
      throws IOException, BundleException {
    int jmxPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      jmxPort = socket.getLocalPort();
    }
    logger.info(
        "Starting node nr: {} for seedHost:seedPort = {}:{}, address: {}, jmxPort: {}.",
        stargateNodeNumber,
        seedHost,
        seedPort,
        stargateHosts.get(stargateNodeNumber),
        jmxPort);
    Starter starter =
        new Starter(
            backend.clusterName(),
            backend.clusterVersion(),
            stargateHosts.get(stargateNodeNumber),
            seedHost,
            seedPort,
            backend.datacenter(),
            backend.rack(),
            backend.isDse(),
            false, // use StargateConfigSnitch to ensure pre-configured test DC name is used
            9043,
            jmxPort);
    System.setProperty("stargate.auth_api_enable_username_token", "true");
    starter.withAuthEnabled(enableAuth).start();
    logger.info("Stargate node nr: {} started successfully", stargateNodeNumber);
    // add to starters only if it start() successfully
    stargateStarters.add(starter);
  }
}
