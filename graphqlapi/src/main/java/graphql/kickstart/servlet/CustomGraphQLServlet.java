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
package graphql.kickstart.servlet;

import graphql.execution.AsyncExecutionStrategy;
import graphql.kickstart.execution.GraphQLObjectMapper;
import graphql.kickstart.execution.GraphQLQueryInvoker;
import graphql.kickstart.execution.config.DefaultExecutionStrategyProvider;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthnzService;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.graphqlservlet.GraphqlCustomContextBuilder;
import io.stargate.graphql.graphqlservlet.StargateGraphqlErrorHandler;
import io.stargate.graphql.schema.SchemaFactory;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomGraphQLServlet extends HttpServlet implements Servlet, EventListener {

  private static final Logger LOG = LoggerFactory.getLogger(CustomGraphQLServlet.class);
  private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("\\w+");

  private final Persistence persistence;
  private final AuthnzService authnzService;
  private final String defaultKeyspace;

  private final ConcurrentMap<String, RequestHandlerReference> keyspaceHandlers;

  public CustomGraphQLServlet(
      Persistence persistence, AuthnzService authnzService) {
    this.persistence = persistence;
    this.authnzService = authnzService;
    DataStore dataStore = DataStore.create(persistence);
    this.defaultKeyspace = findDefaultKeyspace(dataStore);
    this.keyspaceHandlers = initKeyspaceHandlers(persistence, dataStore, authnzService);

    persistence.registerEventListener(this);
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    handle(request, response);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    handle(request, response);
  }

  private void handle(HttpServletRequest request, HttpServletResponse response) {
    String keyspaceName = getKeyspaceName(request);
    if (keyspaceName == null) {
      failOnNoDefaultKeyspace(response);
    } else if (!KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches()) {
      // Do not reflect back the value, to avoid XSS attacks
      response.setStatus(500);
    } else {
      RequestHandlerReference requestHandlerRef = keyspaceHandlers.get(keyspaceName);
      if (requestHandlerRef == null) {
        failOnUnknownKeyspace(keyspaceName, response);
      } else {
        HttpRequestHandler requestHandler;
        try {
          requestHandler = requestHandlerRef.get();
        } catch (Exception e) {
          LOG.error("Error initializing the GraphQL schema", e);
          keyspaceHandlers.remove(keyspaceName, requestHandlerRef);
          response.setStatus(500);
          return;
        }
        try {
          requestHandler.handle(request, response);
        } catch (Exception e) {
          LOG.error("Error processing a GraphQL request", e);
          response.setStatus(500);
        }
      }
    }
  }

  public String getKeyspaceName(HttpServletRequest request) {
    String path = request.getPathInfo();
    if (path == null || path.length() < 2) {
      return defaultKeyspace;
    } else {
      return path.substring(1).toLowerCase();
    }
  }

  private void failOnNoDefaultKeyspace(HttpServletResponse response) {
    String message = "No keyspace specified, and no default keyspace could be determined";
    failWith404(response, message);
  }

  private void failOnUnknownKeyspace(String keyspaceName, HttpServletResponse response) {
    failWith404(response, "Unknown keyspace " + keyspaceName);
  }

  private void failWith404(HttpServletResponse response, String message) {
    response.setStatus(404);
    try {
      // TODO wrap this in a GraphQL error response
      response.getWriter().println(message);
    } catch (IOException e) {
      // ignore
    }
  }

  /** Populate a default keyspace to allow for omitting the keyspace from the path of requests. */
  private static String findDefaultKeyspace(DataStore dataStore) {
    try {
      CompletableFuture<ResultSet> query =
          dataStore.query(
              "select writetime(durable_writes) as wt, keyspace_name from system_schema.keyspaces",
              ConsistencyLevel.LOCAL_QUORUM);

      ResultSet resultSet = query.get();

      Column writetimeColumn = Column.create("wt", Column.Type.Counter);

      // Grab the oldest, non-system keyspace to use as default.
      Optional<Row> first =
          resultSet.rows().stream()
              .filter(r -> !r.isNull("wt"))
              .filter(r -> r.getLong(writetimeColumn.name()) > 0)
              .filter(
                  r -> {
                    String keyspaceName = r.getString("keyspace_name");
                    return !keyspaceName.equals("system")
                        && !keyspaceName.equals("data_endpoint_auth")
                        && !keyspaceName.equals("solr_admin")
                        && !keyspaceName.startsWith("system_")
                        && !keyspaceName.startsWith("dse_");
                  })
              .min(Comparator.comparing(r -> r.getLong(writetimeColumn.name())));

      String defaultKeyspace = first.map(row -> row.getString("keyspace_name")).orElse(null);
      LOG.debug("Using default keyspace {}", defaultKeyspace);
      return defaultKeyspace;
    } catch (Exception e) {
      LOG.warn("Unable to get default keyspace", e);
      return null;
    }
  }

  private static ConcurrentMap<String, RequestHandlerReference> initKeyspaceHandlers(
      Persistence persistence, DataStore dataStore, AuthnzService authnzService) {

    ConcurrentMap<String, RequestHandlerReference> map = new ConcurrentHashMap<>();

    for (Keyspace keyspace : dataStore.schema().keyspaces()) {
      // TODO not sure about toLowerCase, check how case sensitive keyspaces are handled
      String keyspaceName = keyspace.name().toLowerCase();
      LOG.debug("Prepare handler for {}", keyspaceName);
      map.put(
          keyspaceName, new RequestHandlerReference(keyspace, persistence, authnzService));
    }
    return map;
  }

  private void addOrReplaceKeyspaceHandler(
      String keyspaceName, String reason, String... reasonArguments) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Refreshing handler for keyspace {} because {}",
          keyspaceName,
          String.format(reason, reasonArguments));
    }
    try {
      DataStore dataStore = DataStore.create(persistence);
      Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
      if (keyspace == null) {
        // This happens when come from a notification for a keyspace that was just dropped
        LOG.debug("Removing handler for keyspace {} because it was dropped", keyspaceName);
        keyspaceHandlers.remove(keyspaceName);
      } else {
        keyspaceHandlers.put(
            keyspaceName,
            new RequestHandlerReference(keyspace, persistence, authnzService));
      }
      LOG.debug("Done refreshing handler for keyspace {}", keyspaceName);
    } catch (Exception e) {
      LOG.error("Error while refreshing handler for keyspace {}", keyspaceName, e);
    }
  }

  // Schema change callbacks: we refresh a keyspace whenever it gets created or dropped, or anything
  // inside it changes.
  // TODO maybe add debouncing mechanism to amortize quick event bursts

  @Override
  public void onCreateKeyspace(String keyspaceName) {
    addOrReplaceKeyspaceHandler(keyspaceName, "it was created");
  }

  @Override
  public void onDropKeyspace(String keyspaceName) {
    // Note that if the keyspace contained any children, we probably already removed the handler
    // while processing those children's DROP events.
    RequestHandlerReference removed = keyspaceHandlers.remove(keyspaceName);
    if (removed != null) {
      LOG.debug("Removing handler for keyspace {} because it was dropped", keyspaceName);
    }
  }

  @Override
  public void onCreateTable(String keyspaceName, String table) {
    addOrReplaceKeyspaceHandler(keyspaceName, "table %s was created", table);
  }

  @Override
  public void onCreateView(String keyspaceName, String view) {
    addOrReplaceKeyspaceHandler(keyspaceName, "view %s was created", view);
  }

  @Override
  public void onCreateType(String keyspaceName, String type) {
    addOrReplaceKeyspaceHandler(keyspaceName, "type %s was created", type);
  }

  @Override
  public void onCreateFunction(String keyspaceName, String function, List<String> argumentTypes) {
    addOrReplaceKeyspaceHandler(keyspaceName, "function %s was created", function);
  }

  @Override
  public void onCreateAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    addOrReplaceKeyspaceHandler(keyspaceName, "aggregate %s was created", aggregate);
  }

  @Override
  public void onAlterTable(String keyspaceName, String table) {
    addOrReplaceKeyspaceHandler(keyspaceName, "table %s was altered", table);
  }

  @Override
  public void onAlterView(String keyspaceName, String view) {
    addOrReplaceKeyspaceHandler(keyspaceName, "view %s was altered", view);
  }

  @Override
  public void onAlterType(String keyspaceName, String type) {
    addOrReplaceKeyspaceHandler(keyspaceName, "type %s was altered", type);
  }

  @Override
  public void onAlterFunction(String keyspaceName, String function, List<String> argumentTypes) {
    addOrReplaceKeyspaceHandler(keyspaceName, "function %s was altered", function);
  }

  @Override
  public void onAlterAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    addOrReplaceKeyspaceHandler(keyspaceName, "aggregate %s was altered", aggregate);
  }

  @Override
  public void onDropTable(String keyspaceName, String table) {
    addOrReplaceKeyspaceHandler(keyspaceName, "table %s was dropped", table);
  }

  @Override
  public void onDropView(String keyspaceName, String view) {
    addOrReplaceKeyspaceHandler(keyspaceName, "view %s was dropped", view);
  }

  @Override
  public void onDropType(String keyspaceName, String type) {
    addOrReplaceKeyspaceHandler(keyspaceName, "type %s was dropped", type);
  }

  @Override
  public void onDropFunction(String keyspaceName, String function, List<String> argumentTypes) {
    addOrReplaceKeyspaceHandler(keyspaceName, "function %s was dropped", function);
  }

  @Override
  public void onDropAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    addOrReplaceKeyspaceHandler(keyspaceName, "aggregate %s was dropped", aggregate);
  }

  /**
   * Holds an {@link HttpRequestHandlerImpl} instance lazily: the construction of the GraphQL schema
   * will only happen the first time the keyspace is queried.
   */
  static class RequestHandlerReference {
    private final Keyspace keyspace;
    private final Persistence persistence;
    private final AuthnzService authnzService;

    private volatile HttpRequestHandlerImpl handler;

    RequestHandlerReference(
        Keyspace keyspace, Persistence persistence, AuthnzService authnzService) {
      this.keyspace = keyspace;
      this.persistence = persistence;
      this.authnzService = authnzService;
    }

    HttpRequestHandler get() {
      // Double-checked locking
      HttpRequestHandlerImpl result = handler;
      if (handler == null) {
        synchronized (this) {
          if (handler == null) {
            handler = result = computeHandler();
          }
        }
      }
      return result;
    }

    private HttpRequestHandlerImpl computeHandler() {
      GraphQLSchema schema =
          SchemaFactory.newDmlSchema(persistence, authnzService, keyspace);
      GraphQLConfiguration configuration =
          GraphQLConfiguration.with(schema)
              .with(
                  // Queries and mutations within the same request run in parallel
                  GraphQLQueryInvoker.newBuilder()
                      .withExecutionStrategyProvider(
                          new DefaultExecutionStrategyProvider(new AsyncExecutionStrategy()))
                      .build())
              .with(new GraphqlCustomContextBuilder())
              .with(
                  GraphQLObjectMapper.newBuilder()
                      .withGraphQLErrorHandler(new StargateGraphqlErrorHandler())
                      .build())
              .build();
      return new HttpRequestHandlerImpl(configuration);
    }
  }
}
