package io.stargate.graphql.schema.fetchers.ddl;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;

public abstract class TableFetcher extends DdlQueryFetcher {

  protected TableFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      AuthenticationPrincipal authenticationPrincipal)
      throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String tableName = dataFetchingEnvironment.getArgument("tableName");
    Scope scope = null;

    if (this instanceof AlterTableAddFetcher || this instanceof AlterTableDropFetcher) {
      scope = Scope.ALTER;
    } else if (this instanceof CreateTableFetcher) {
      scope = Scope.CREATE;
    } else if (this instanceof DropTableFetcher) {
      scope = Scope.DROP;
    }

    authorizationService.authorizeSchemaWrite(
        authenticationPrincipal, keyspaceName, tableName, scope, SourceAPI.GRAPHQL);

    return buildQuery(dataFetchingEnvironment, builder, keyspaceName, tableName);
  }

  protected abstract Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName);
}
