package graphql.kickstart.servlet;

import graphql.kickstart.execution.GraphQLObjectMapper;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.graphql.core.GqlKeyspaceSchema;
import io.stargate.graphql.graphqlservlet.CassandraUnboxingGraphqlErrorHandler;
import io.stargate.graphql.graphqlservlet.GraphqlCustomContextBuilder;
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

  private final Persistence<?, ?, ?> persistence;
  private final AuthenticationService authenticationService;
  private final String defaultKeyspace;

  private final ConcurrentMap<String, HttpRequestHandler> keyspaceHandlers;

  public CustomGraphQLServlet(
      Persistence<?, ?, ?> persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
    DataStore dataStore = persistence.newDataStore(null, null);
    this.defaultKeyspace = findDefaultKeyspace(dataStore);
    this.keyspaceHandlers = initKeyspaceHandlers(persistence, dataStore, authenticationService);

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
      HttpRequestHandler requestHandler = keyspaceHandlers.get(keyspaceName);
      if (requestHandler == null) {
        failOnUnknownKeyspace(keyspaceName, response);
      } else {
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
              Optional.of(ConsistencyLevel.LOCAL_QUORUM));

      ResultSet resultSet = query.get();

      Column writetimeColumn = Column.create("wt", Column.Type.Counter);

      // Grab the oldest, non-system keyspace to use as default.
      Optional<Row> first =
          resultSet.rows().stream()
              .filter(r -> r.has("wt"))
              .filter(r -> r.getLong(writetimeColumn) > 0)
              .filter(
                  r -> {
                    String keyspaceName = r.getString("keyspace_name");
                    return !keyspaceName.equals("system")
                        && !keyspaceName.equals("data_endpoint_auth")
                        && !keyspaceName.equals("solr_admin")
                        && !keyspaceName.startsWith("system_")
                        && !keyspaceName.startsWith("dse_");
                  })
              .min(Comparator.comparing(r -> r.getLong(writetimeColumn)));

      String defaultKeyspace = first.map(row -> row.getString("keyspace_name")).orElse(null);
      LOG.debug("Using default keyspace {}", defaultKeyspace);
      return defaultKeyspace;
    } catch (Exception e) {
      LOG.warn("Unable to get default keyspace", e);
      return null;
    }
  }

  private static ConcurrentMap<String, HttpRequestHandler> initKeyspaceHandlers(
      Persistence<?, ?, ?> persistence,
      DataStore dataStore,
      AuthenticationService authenticationService) {

    ConcurrentMap<String, HttpRequestHandler> map = new ConcurrentHashMap<>();

    for (Keyspace keyspace : dataStore.schema().keyspaces()) {
      // TODO not sure about toLowerCase, check how case sensitive keyspaces are handled
      String keyspaceName = keyspace.name().toLowerCase();
      try {
        HttpRequestHandler handler =
            buildKeyspaceHandler(keyspace, persistence, authenticationService);
        map.put(keyspaceName, handler);
        LOG.debug("Added handler for {}", keyspaceName);
      } catch (Exception e) {
        LOG.warn("Could not create handler for " + keyspaceName, e);
      }
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
      DataStore dataStore = persistence.newDataStore(null, null);
      Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
      keyspaceHandlers.put(
          keyspaceName, buildKeyspaceHandler(keyspace, persistence, authenticationService));
      LOG.debug("Done refreshing handler for keyspace {}", keyspaceName);
    } catch (Exception e) {
      LOG.error("Error while refreshing handler for keyspace {}", keyspaceName, e);
    }
  }

  private static HttpRequestHandler buildKeyspaceHandler(
      Keyspace keyspace,
      Persistence<?, ?, ?> persistence,
      AuthenticationService authenticationService) {
    GraphQLSchema schema =
        new GqlKeyspaceSchema(persistence, authenticationService, keyspace).build().build();
    GraphQLConfiguration configuration =
        GraphQLConfiguration.with(schema)
            .with(new GraphqlCustomContextBuilder())
            .with(
                GraphQLObjectMapper.newBuilder()
                    .withGraphQLErrorHandler(new CassandraUnboxingGraphqlErrorHandler())
                    .build())
            .build();
    return new HttpRequestHandlerImpl(configuration);
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
    HttpRequestHandler removed = keyspaceHandlers.remove(keyspaceName);
    if (removed != null) {
      LOG.debug("Removing handler for keyspace {} because it was dropped", keyspaceName);
    } else {
      LOG.warn(
          "Keyspace {} was dropped, but we didn't have a handler for it,"
              + "ignoring but that's unexpected",
          keyspaceName);
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
}
