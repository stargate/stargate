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
package io.stargate.sgv2.graphql.impl;

import io.dropwizard.Configuration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphqlServiceServerConfiguration extends Configuration {
  public StargateConfiguration stargate = new StargateConfiguration();

  /** Value class definitions */
  static class StargateConfiguration {
    public BridgeConfiguration bridge = new BridgeConfiguration();
  }

  public static class BridgeConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeConfiguration.class);

    public String host = "localhost";
    public int port = 8091;

    // !!! TODO: probably should default to true
    public boolean useTls = false;

    public ManagedChannel buildChannel() {
      LOG.info("Bridge endpoint for GraphqlService v2 is to use: {}", this);
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
