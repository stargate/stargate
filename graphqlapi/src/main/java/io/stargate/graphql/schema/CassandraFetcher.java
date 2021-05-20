package io.stargate.graphql.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.graphql.web.StargateGraphqlContext;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** Base class for fetchers that access the Cassandra backend. It also handles authentication. */
public abstract class CassandraFetcher<ResultT> implements DataFetcher<ResultT> {

  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final Parameters DEFAULT_PARAMETERS =
      Parameters.builder()
          .pageSize(DEFAULT_PAGE_SIZE)
          .consistencyLevel(DEFAULT_CONSISTENCY)
          .serialConsistencyLevel(DEFAULT_SERIAL_CONSISTENCY)
          .build();

  @Override
  public final ResultT get(DataFetchingEnvironment environment) throws Exception {
    StargateGraphqlContext context = environment.getContext();

    AuthenticationSubject authenticationSubject = context.getSubject();

    Parameters parameters = getDatastoreParameters(environment);
    DataStoreOptions dataStoreOptions =
        DataStoreOptions.builder()
            .putAllCustomProperties(context.getAllHeaders())
            .defaultParameters(parameters)
            .alwaysPrepareQueries(true)
            .build();
    DataStore dataStore =
        context.getDataStoreFactory().create(authenticationSubject.asUser(), dataStoreOptions);
    return get(environment, dataStore, context);
  }

  protected Parameters getDatastoreParameters(DataFetchingEnvironment environment) {
    return DEFAULT_PARAMETERS;
  }

  protected abstract ResultT get(
      DataFetchingEnvironment environment, DataStore dataStore, StargateGraphqlContext context)
      throws Exception;
}
