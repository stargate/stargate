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
package io.stargate.web.impl;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;

/**
 * Configuration class for StargateV1 Docs API.
 *
 * <p>The only interesting thing is to apply Docs API port override via System properties:
 *
 * <pre>
 *  -Ddw.docsApiPortOverride=8099
 * </pre>
 *
 * (although technically it is also possible to add root-level value in {@code
 * src/main/resources/config.yaml} that could simply use the regular YAML overide of
 * "server.connector.port: 8099" instead)
 */
public class RestApiServerConfiguration extends Configuration {
  private int docsApiPortOverride = -1;

  public void docsApiPortOverride(int port) {
    docsApiPortOverride = port;
    ServerFactory serverF = this.getServerFactory();
    // It's actually "RestApiServerFactory" but that extends SimpleServerFactory so:
    HttpConnectorFactory connF =
        (HttpConnectorFactory) ((SimpleServerFactory) serverF).getConnector();
    connF.setPort(port);
  }

  public int getDocsApiPortOverride() {
    return docsApiPortOverride;
  }
}
