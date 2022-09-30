package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.StargateGraphqlContext;

public class DeleteMutationFetcher extends MutationFetcher {

  public DeleteMutationFetcher(Table table, NameMapping nameMapping) {
    super(table, nameMapping);
  }

  @Override
  protected BoundQuery buildQuery(
      DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws UnauthorizedException {

    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    BoundQuery bound =
        context
            .getDataStore()
            .queryBuilder()
            .delete()
            .from(table.keyspace(), table.name())
            .where(buildClause(table, environment))
            .ifs(buildConditions(table, environment.getArgument("ifCondition")))
            .ifExists(ifExists)
            .build()
            .bind();

    assert bound instanceof BoundDelete;
    context
        .getAuthorizationService()
        .authorizeDataWrite(
            context.getSubject(),
            table.keyspace(),
            table.name(),
            TypedKeyValue.forDML((BoundDelete) bound),
            Scope.DELETE,
            SourceAPI.GRAPHQL);
    return bound;
  }
}
