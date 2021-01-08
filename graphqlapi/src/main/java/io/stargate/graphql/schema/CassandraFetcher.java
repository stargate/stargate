package io.stargate.graphql.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.graphql.web.HttpAwareContext;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** Base class for fetchers that access the Cassandra backend. It also handles authentication. */
public abstract class CassandraFetcher<ResultT> implements DataFetcher<ResultT> {
  protected final AuthenticationService authenticationService;
  protected final AuthorizationService authorizationService;
  protected final DataStoreFactory dataStoreFactory;

  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final Parameters DEFAULT_PARAMETERS =
      Parameters.builder()
          .pageSize(DEFAULT_PAGE_SIZE)
          .consistencyLevel(DEFAULT_CONSISTENCY)
          .serialConsistencyLevel(DEFAULT_SERIAL_CONSISTENCY)
          .build();

  public CassandraFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
  }

  @Override
  public final ResultT get(DataFetchingEnvironment environment) throws Exception {
    HttpAwareContext httpAwareContext = environment.getContext();

    String token = httpAwareContext.getAuthToken();
    AuthenticationSubject authenticationSubject =
        authenticationService.validateToken(token, httpAwareContext.getAllHeaders());

    Parameters parameters = getDatastoreParameters(environment);
    DataStoreOptions dataStoreOptions =
        DataStoreOptions.builder()
            .putAllCustomProperties(httpAwareContext.getAllHeaders())
            .defaultParameters(parameters)
            .alwaysPrepareQueries(true)
            .build();
    DataStore dataStore = dataStoreFactory.create(authenticationSubject.asUser(), dataStoreOptions);
    return get(environment, dataStore, authenticationSubject);
  }

  protected Parameters getDatastoreParameters(DataFetchingEnvironment environment) {
    return DEFAULT_PARAMETERS;
  }

  protected abstract ResultT get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception;
}
