package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected Query buildQuery(DataFetchingEnvironment environment, StargateGraphqlContext context) {

    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    return new QueryBuilder()
        .delete()
        .from(keyspaceName, table.getName())
        .where(buildClause(table, environment))
        .ifs(buildConditions(environment.getArgument("ifCondition")))
        .ifExists(ifExists)
        .parameters(buildParameters(environment))
        .build();
  }
}
