package io.stargate.graphql.schema.fetchers.dml;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.*;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(
        table,
        nameMapping,
        persistence,
        authenticationService,
        authorizationService,
        dataStoreFactory);
  }

  @Override
  protected BoundQuery buildQuery(DataFetchingEnvironment environment, DataStore dataStore)
      throws UnauthorizedException {

    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    BoundQuery bound =
        dataStore
            .queryBuilder()
            .delete()
            .from(table.keyspace(), table.name())
            .where(buildClause(table, environment))
            .ifs(buildConditions(table, environment.getArgument("ifCondition")))
            .ifExists(ifExists)
            .build()
            .bind();

    assert bound instanceof BoundDelete;
    authorizationService.authorizeDataWrite(
        token,
        table.keyspace(),
        table.name(),
        TypedKeyValue.forDML((BoundDelete) bound),
        Scope.DELETE,
        SourceAPI.GRAPHQL);
    return bound;
  }
}
