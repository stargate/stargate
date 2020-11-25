package io.stargate.graphql.schema.fetchers.ddl;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.graphql.web.HttpAwareContext;

public abstract class TableFetcher extends DdlQueryFetcher {

  protected TableFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  String getQuery(DataFetchingEnvironment dataFetchingEnvironment) throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String tableName = dataFetchingEnvironment.getArgument("tableName");

    HttpAwareContext httpAwareContext = dataFetchingEnvironment.getContext();
    String token = httpAwareContext.getAuthToken();

    Scope scope = null;

    if (this instanceof AlterTableAddFetcher || this instanceof AlterTableDropFetcher) {
      scope = Scope.ALTER;
    } else if (this instanceof CreateTableFetcher) {
      scope = Scope.CREATE;
    } else if (this instanceof DropTableFetcher) {
      scope = Scope.DROP;
    }

    authorizationService.authorizeSchemaWrite(token, keyspaceName, tableName, scope);

    return getQuery(dataFetchingEnvironment, keyspaceName, tableName);
  }

  abstract String getQuery(
      DataFetchingEnvironment dataFetchingEnvironment, String keyspaceName, String tableName);
}
