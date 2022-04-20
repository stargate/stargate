package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

public abstract class IndexFetcher extends DdlQueryFetcher {

  @Override
  protected Query buildQuery(DataFetchingEnvironment environment, StargateGraphqlContext context) {
    String keyspaceName = environment.getArgument("keyspaceName");
    String tableName = environment.getArgument("tableName");
    return buildQuery(environment, keyspaceName, tableName);
  }

  protected abstract Query buildQuery(
      DataFetchingEnvironment environment, String keyspaceName, String tableName);
}
