package io.stargate.graphql.schema.fetchers.dml;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import java.util.Map;

public abstract class MutationFetcher extends DmlFetcher {

  protected MutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService) {
    super(table, nameMapping, persistence, authenticationService);
  }

  @Override
  protected Map<String, Object> get(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception {
    String statement = buildStatement(environment, dataStore);
    dataStore.query(statement).get();
    return ImmutableMap.of("value", environment.getArgument("value"));
  }

  protected abstract String buildStatement(
      DataFetchingEnvironment environment, DataStore dataStore);
}
