package graphql.kickstart.servlet;

import graphql.kickstart.execution.GraphQLObjectMapper;
import graphql.kickstart.servlet.core.internal.GraphQLThreadFactory;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.graphql.core.GqlKeyspaceSchema;
import io.stargate.graphql.graphqlservlet.CassandraUnboxingGraphqlErrorHandler;
import io.stargate.graphql.graphqlservlet.GraphqlCustomContextBuilder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.AsyncContext;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomGraphQLServlet extends HttpServlet implements Servlet {
  private static final Logger log = LoggerFactory.getLogger(CustomGraphQLServlet.class);
  private final boolean isAsyncServletModeEnabled;
  private final Persistence persistence;
  private AuthenticationService authenticationService;
  private ExecutorService asyncExecutor = Executors.newCachedThreadPool(new GraphQLThreadFactory());
  private ScheduledExecutorService refreshExecutor;
  private static String DEFAULT_KEYSPACE;

  private Map<String, HttpRequestHandlerImpl> handlerMap;

  public CustomGraphQLServlet(
      boolean isAsyncServletModeEnabled,
      Persistence persistence,
      AuthenticationService authenticationService) {
    this.isAsyncServletModeEnabled = isAsyncServletModeEnabled;
    this.persistence = persistence;
    this.authenticationService = authenticationService;

    refreshExecutor = Executors.newSingleThreadScheduledExecutor();
    refreshExecutor.scheduleWithFixedDelay(this::schemaRefresh, 0, 10, TimeUnit.SECONDS);
  }

  public void init() {
    if (this.handlerMap == null) {
      schemaRefresh();
    }
  }

  private void schemaRefresh() {
    synchronized (this) {
      DataStore dataStore = persistence.newDataStore(null, null);

      if (DEFAULT_KEYSPACE == null || "".equals(DEFAULT_KEYSPACE)) {
        populateDefaultKeyspace(dataStore);
      }

      Map map = new HashMap();
      for (Keyspace keyspace : dataStore.schema().keyspaces()) {
        try {
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
          HttpRequestHandlerImpl handler = new HttpRequestHandlerImpl(configuration);
          map.put(keyspace.name().toLowerCase(), handler);
        } catch (Exception e) {
          log.info("Could not build schema {}", keyspace.name());
        }
      }
      handlerMap = map;
    }
  }

  /**
   * Populate a default keyspace to allow for omitting the keyspace from the path of requests.
   *
   * @param dataStore
   */
  private void populateDefaultKeyspace(DataStore dataStore) {
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

      DEFAULT_KEYSPACE = first.map(row -> row.getString("keyspace_name")).orElse(null);
    } catch (Exception e) {
      log.warn("Unable to get default keyspace", e);
    }
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
    this.init();

    this.doRequestAsync(req, resp, this.handlerMap.get(getKeyspace(req)));
  }

  public String getKeyspace(HttpServletRequest request) {
    String path = request.getPathInfo();
    if (path == null) {
      return DEFAULT_KEYSPACE;
    }

    return path.substring(1).toLowerCase();
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
    this.init();

    this.doRequestAsync(req, resp, this.handlerMap.get(getKeyspace(req)));
  }

  private void doRequestAsync(
      HttpServletRequest request, HttpServletResponse response, HttpRequestHandler handler) {
    if (handler == null) {
      response.setStatus(404);
      throw new RuntimeException("Could not find keyspace");
    }
    if (this.isAsyncServletModeEnabled) {
      AsyncContext asyncContext = request.startAsync(request, response);
      HttpServletRequest asyncRequest = (HttpServletRequest) asyncContext.getRequest();
      HttpServletResponse asyncResponse = (HttpServletResponse) asyncContext.getResponse();
      this.asyncExecutor.execute(
          () -> {
            this.doRequest(asyncRequest, asyncResponse, handler, asyncContext);
          });
    } else {
      this.doRequest(request, response, handler, (AsyncContext) null);
    }
  }

  private void doRequest(
      HttpServletRequest request,
      HttpServletResponse response,
      HttpRequestHandler handler,
      AsyncContext asyncContext) {
    try {
      handler.handle(request, response);
    } catch (Throwable t) {
      log.error("Error executing GraphQL request!", t);
    } finally {
      if (asyncContext != null) {
        asyncContext.complete();
      }
    }
  }
}
