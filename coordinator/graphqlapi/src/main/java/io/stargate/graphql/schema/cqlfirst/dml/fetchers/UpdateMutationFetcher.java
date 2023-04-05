package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.TtlFromOptionsExtractor.getTTL;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Modification;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.Value;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateMutationFetcher extends MutationFetcher {

  public UpdateMutationFetcher(Table table, NameMapping nameMapping) {
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

    BoundQuery query =
        context
            .getDataStore()
            .queryBuilder()
            .update(table.keyspace(), table.name())
            .ttl(getTTL(environment))
            .value(buildAssignments(table, environment))
            .where(buildPkCKWhere(table, environment))
            .ifs(buildConditions(table, environment.getArgument("ifCondition")))
            .ifExists(ifExists)
            .build()
            .bind();

    context
        .getAuthorizationService()
        .authorizeDataWrite(
            context.getSubject(),
            table.keyspace(),
            table.name(),
            TypedKeyValue.forDML((BoundDMLQuery) query),
            Scope.MODIFY,
            SourceAPI.GRAPHQL);

    return query;
  }

  private List<BuiltCondition> buildPkCKWhere(Table table, DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<BuiltCondition> relations = new ArrayList<>();

    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = dbColumnGetter.getColumn(table, entry.getKey());
      if (table.partitionKeyColumns().contains(column)
          || table.clusteringKeyColumns().contains(column)) {
        relations.add(
            BuiltCondition.of(column.name(), Predicate.EQ, toDBValue(column, entry.getValue())));
      }
    }
    return relations;
  }

  private List<ValueModifier> buildAssignments(Table table, DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<ValueModifier> assignments = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = dbColumnGetter.getColumn(table, entry.getKey());
      if (!(table.partitionKeyColumns().contains(column)
          || table.clusteringKeyColumns().contains(column))) {
        Modification.Operation operation =
            column.type() == Column.Type.Counter
                ? Modification.Operation.INCREMENT
                : Modification.Operation.SET;
        assignments.add(
            ValueModifier.of(
                column.name(), Value.of(toDBValue(column, entry.getValue())), operation));
      }
    }
    return assignments;
  }
}
