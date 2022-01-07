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
package io.stargate.sgv2.restsvc.driver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import java.net.InetSocketAddress;

/** Builds the Java driver session that will be used to communicate with the CQL bridge. */
public class CqlSessionFactory {

  public static CqlSession newInstance() {
    String host = System.getProperty("stargate.cql.internal_listen_address", "127.0.0.2");
    int port = Integer.getInteger("stargate.cql.internal_port", 9044);
    InetSocketAddress rpcAddress = new InetSocketAddress(host, port);

    // Static config options from `src/main/resources/java-driver-bridge.conf`
    DriverConfigLoader configLoader = DriverConfigLoader.fromClasspath("java-driver-bridge");

    return CqlSession.builder()
        .withConfigLoader(configLoader)
        .addContactPoint(rpcAddress)
        .withNodeFilter(node -> node.getEndPoint().resolve().equals(rpcAddress))
        .build();
  }
}
