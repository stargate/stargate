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
package io.stargate.sgv2.restsvc.impl;

import io.dropwizard.Configuration;

public class RestServiceServerConfiguration extends Configuration {
  public StargateConfig stargate = new StargateConfig();

  /** Value class definitions */
  static class StargateConfig {
    public EndpointConfig grpc = new EndpointConfig("localhost", 8091, false);
  }

  static class EndpointConfig {
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

    @Override
    public String toString() {
      return String.format("%s://%s:%s", useTls ? "https" : "http", host, port);
    }
  }
}
