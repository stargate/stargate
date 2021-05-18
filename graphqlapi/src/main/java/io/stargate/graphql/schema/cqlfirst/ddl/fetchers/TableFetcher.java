package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.graphql.web.StargateGraphqlContext;

public abstract class TableFetcher extends DdlQueryFetcher {

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment environment, QueryBuilder builder, StargateGraphqlContext context)
      throws UnauthorizedException {
    String keyspaceName = environment.getArgument("keyspaceName");
    String tableName = environment.getArgument("tableName");
    Scope scope = null;

    if (this instanceof AlterTableAddFetcher || this instanceof AlterTableDropFetcher) {
      scope = Scope.ALTER;
    } else if (this instanceof CreateTableFetcher) {
      scope = Scope.CREATE;
    } else if (this instanceof DropTableFetcher) {
      scope = Scope.DROP;
    }

    context
        .getAuthorizationService()
        .authorizeSchemaWrite(
            context.getSubject(),
            keyspaceName,
            tableName,
            scope,
            SourceAPI.GRAPHQL,
            ResourceKind.TABLE);

    return buildQuery(environment, builder, keyspaceName, tableName);
  }

  protected abstract Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName);
}
