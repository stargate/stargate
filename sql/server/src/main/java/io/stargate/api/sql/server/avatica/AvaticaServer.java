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
package io.stargate.api.sql.server.avatica;

import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import java.io.IOException;
import java.util.Collections;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.ServerCustomizer;
import org.eclipse.jetty.server.AbstractNetworkConnector;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;

public class AvaticaServer {
  public static final int PORT = Integer.getInteger("stargate.sql.avatica.port", 8765);
  public static final String LISTEN_ADDRESS = System.getProperty("stargate.listen_address", null);

  private final ClassLoader contextClassLoader = getClass().getClassLoader();
  private final HttpServer server;

  public AvaticaServer(Persistence backend, AuthenticationService authenticator) {
    Meta meta = new StargateMeta(user -> DataStore.create(backend, user), authenticator);
    Service service = new LocalService(meta);

    ServerCustomizer<Server> customizer =
        s -> {
          for (Connector c : s.getConnectors()) {
            if (c instanceof AbstractNetworkConnector) {
              ((AbstractNetworkConnector) c).setHost(LISTEN_ADDRESS);
            }
          }
        };

    server =
        new HttpServer.Builder<Server>()
            .withPort(PORT)
            .withHandler(new Handler(service))
            .withServerCustomizers(Collections.singletonList(customizer), Server.class)
            .build();
  }

  public void start() {
    server.start();
  }

  public void stop() {
    server.stop();
  }

  private final class Handler extends AvaticaProtobufHandler {
    public Handler(Service service) {
      super(service);
    }

    @Override
    public void handle(
        String target,
        Request baseRequest,
        HttpServletRequest request,
        HttpServletResponse response)
        throws IOException, ServletException {
      Thread currentThread = Thread.currentThread();
      ClassLoader ldr = currentThread.getContextClassLoader();
      try {
        // Note: Apache Calcite expects to find certain dependencies using the thread context class
        // loader.
        // However, in our OSGi framework, the default context class loader is the system class
        // loader,
        // which does not contain module jars.
        currentThread.setContextClassLoader(contextClassLoader);
        super.handle(target, baseRequest, request, response);
      } finally {
        currentThread.setContextClassLoader(ldr);
      }
    }
  }
}
