package io.stargate.graphql.schema.fetchers.ddl;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import java.util.Collections;
import java.util.Map;

public abstract class TableFetcher extends DdlQueryFetcher {
  private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

  protected TableFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      AuthenticationSubject authenticationSubject)
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
        authenticationSubject, keyspaceName, tableName, scope, SourceAPI.GRAPHQL, EMPTY_HEADERS);

    return buildQuery(dataFetchingEnvironment, builder, keyspaceName, tableName);
  }

  protected abstract Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName);
}
