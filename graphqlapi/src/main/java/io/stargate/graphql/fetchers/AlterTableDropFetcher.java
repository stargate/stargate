package io.stargate.graphql.fetchers;

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableDropColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.List;

public class AlterTableDropFetcher
    implements io.stargate.graphql.fetchers.SchemaFetcher, DataFetcher {
  private final Persistence persistence;
  private AuthenticationService authenticationService;

  public AlterTableDropFetcher(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  @Override
  public Object get(DataFetchingEnvironment environment) throws Exception {
    HTTPAwareContextImpl httpAwareContext = environment.getContext();

    String token = httpAwareContext.getAuthToken();
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = persistence.newQueryState(clientState);
    DataStore dataStore = persistence.newDataStore(queryState, null);

    return dataStore.query(getQuery(environment)).thenApply(result -> true);
  }

  @Override
  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    AlterTableStart start =
        SchemaBuilder.alterTable(
            (String) dataFetchingEnvironment.getArgument("keyspaceName"),
            (String) dataFetchingEnvironment.getArgument("tableName"));

    List<String> toDrop = dataFetchingEnvironment.getArgument("toDrop");
    if (toDrop.isEmpty()) {
      // TODO see if we can enforce that through the schema instead
      throw new IllegalArgumentException("toDrop must contain at least one element");
    }
    AlterTableDropColumnEnd table = null;
    for (String column : toDrop) {
      if (table != null) {
        table = table.dropColumn(column);
      } else {
        table = start.dropColumn(column);
      }
    }
    return table.build().getQuery();
  }
}
