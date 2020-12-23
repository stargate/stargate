package io.stargate.graphql.schema.fetchers.dml;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(
      Table table,
      NameMapping nameMapping,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(table, nameMapping, authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected BoundQuery buildQuery(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationPrincipal authenticationPrincipal)
      throws UnauthorizedException {

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
        authenticationPrincipal,
        table.keyspace(),
        table.name(),
        TypedKeyValue.forDML((BoundDelete) bound),
        Scope.DELETE,
        SourceAPI.GRAPHQL);
    return bound;
  }
}
