package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkNotNull;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.graphql.web.StargateGraphqlContext;

public abstract class IndexFetcher extends DdlQueryFetcher {

  protected final Scope scope;

  protected IndexFetcher(Scope scope) {
    checkNotNull(scope, "No Scope provided");
    this.scope = scope;
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment environment, QueryBuilder builder, StargateGraphqlContext context)
      throws UnauthorizedException {
    String keyspaceName = environment.getArgument("keyspaceName");
    String tableName = environment.getArgument("tableName");

    context
        .getAuthorizationService()
        .authorizeSchemaWrite(
            context.getSubject(),
            keyspaceName,
            tableName,
            scope,
            SourceAPI.GRAPHQL,
            ResourceKind.INDEX);

    return buildQuery(environment, builder, keyspaceName, tableName);
  }

  protected abstract Query<?> buildQuery(
      DataFetchingEnvironment environment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName);
}
