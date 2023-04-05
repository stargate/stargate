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
package io.stargate.graphql.web;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.jetty.ContextRoutingHandler;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Environment;
import java.util.Collections;
import java.util.Map;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;

/**
 * @see "META-INF/services/io.dropwizard.server.ServerFactory"
 * @see "graphqlapi-config.yaml"
 */
@JsonTypeName("graphql-api")
public class GraphqlApiServerFactory extends SimpleServerFactory {
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

    final Connector conn =
        getConnector().build(server, environment.metrics(), environment.getName(), null);

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
