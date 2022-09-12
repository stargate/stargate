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

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.jetty.ContextRoutingHandler;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Environment;
import java.util.Collections;
import java.util.Map;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see "META-INF/services/io.dropwizard.server.ServerFactory"
 * @see "restapi-config.yaml"
 */
@JsonTypeName("rest-api")
public class RestApiServerFactory extends SimpleServerFactory {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public static final String SYSPROP_PORT_OVERRIDE_SGV1_REST = "stargate.rest.portOverrideV1";

  @Override
  public Server build(Environment environment) {
    configure(environment);

    printBanner(environment.getName());
    final ThreadPool threadPool = createThreadPool(environment.metrics());
    final Server server = buildServer(environment.lifecycle(), threadPool);

    final Handler applicationHandler =
        createAppServlet(
            server,
            environment.jersey(),
            environment.getObjectMapper(),
            environment.getValidator(),
            environment.getApplicationContext(),
            environment.getJerseyServletContainer(),
            environment.metrics());

    HttpConnectorFactory connectorFactory = (HttpConnectorFactory) getConnector();

    String portOverrideStr = System.getProperty(SYSPROP_PORT_OVERRIDE_SGV1_REST);
    if (portOverrideStr != null && !portOverrideStr.isEmpty()) {
      logger.info(
          "Overriding StargateV1 restapi and docsapi port. System property '{}' set to {}",
          SYSPROP_PORT_OVERRIDE_SGV1_REST,
          portOverrideStr);
      connectorFactory.setPort(Integer.parseInt(portOverrideStr));
    }

    final Connector conn =
        connectorFactory.build(server, environment.metrics(), environment.getName(), null);

    server.addConnector(conn);

    final Map<String, Handler> handlers =
        Collections.singletonMap(getApplicationContextPath(), applicationHandler);
    final ContextRoutingHandler routingHandler = new ContextRoutingHandler(handlers);
    final Handler gzipHandler = buildGzipHandler(routingHandler);
    server.setHandler(addStatsHandler(addRequestLog(server, gzipHandler, environment.getName())));
    return server;
  }

  @Override
  public void configure(Environment environment) {
    environment.getApplicationContext().setContextPath(getApplicationContextPath());
  }
}
