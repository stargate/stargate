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
package io.stargate.sgv2.dynamosvc.impl;

import io.dropwizard.Configuration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoServiceServerConfiguration extends Configuration {
  public StargateConfig stargate = new StargateConfig();

  /** Value class definitions */
  static class StargateConfig {
    public EndpointConfig bridge = new EndpointConfig("localhost", 8091, false);
  }

  static class EndpointConfig {
    private static final Logger LOG = LoggerFactory.getLogger(EndpointConfig.class);

    public String host = "localhost";
    public int port = -1;

    // !!! TODO: probably should default to true
    public boolean useTls = false;

    protected EndpointConfig() {}

    public EndpointConfig(String host, int port, boolean useTls) {
      this.host = host;
      this.port = port;
      this.useTls = useTls;
    }

    public ManagedChannel buildChannel() {
      LOG.info("Bridge endpoint for RestService v2 is to use: {}", this);
      ManagedChannelBuilder<?> builder =
          ManagedChannelBuilder.forAddress(host, port).directExecutor();
      if (useTls) {
        builder = builder.useTransportSecurity();
      } else {
        builder = builder.usePlaintext();
      }
      return builder.build();
    }

    @Override
    public String toString() {
      return String.format("%s://%s:%s", useTls ? "https" : "http", host, port);
    }
  }
}
