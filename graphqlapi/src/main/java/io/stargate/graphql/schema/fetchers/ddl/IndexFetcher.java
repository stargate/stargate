package io.stargate.graphql.schema.fetchers.ddl;

import static com.datastax.oss.driver.shaded.guava.common.base.Preconditions.checkNotNull;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.*;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.graphql.web.HttpAwareContext;

public abstract class IndexFetcher extends DdlQueryFetcher {

  protected final Scope scope;

  protected IndexFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory,
      Scope scope) {
    super(authenticationService, authorizationService, dataStoreFactory);

    checkNotNull(scope, "No Scope provided");
    this.scope = scope;
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment, QueryBuilder builder)
      throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String tableName = dataFetchingEnvironment.getArgument("tableName");

    HttpAwareContext httpAwareContext = dataFetchingEnvironment.getContext();
    String token = httpAwareContext.getAuthToken();

    authorizationService.authorizeSchemaWrite(
        token, keyspaceName, tableName, scope, SourceAPI.GRAPHQL);

    return buildQuery(dataFetchingEnvironment, builder, keyspaceName, tableName);
  }

  protected abstract Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName);
}
