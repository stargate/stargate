package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.api.common.cql.builder.ValueModifier;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateMutationFetcher extends MutationFetcher {

  public UpdateMutationFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected QueryOuterClass.Query buildQuery(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    boolean ifExists =
        environment.containsArgument("ifExists")
            && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists");

    return new QueryBuilder()
        .update(keyspaceName, table.getName())
        .ttl(getTtl(environment))
        .value(buildAssignments(environment))
        .where(buildPrimaryKeyWhere(environment))
        .ifs(buildConditions(environment.getArgument("ifCondition")))
        .ifExists(ifExists)
        .parameters(buildParameters(environment))
        .build();
  }

  private List<ValueModifier> buildAssignments(DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<ValueModifier> assignments = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      ColumnSpec column = dbColumnGetter.getColumn(table, entry.getKey());
      if (!isPrimaryKey(column)) {
        ValueModifier.Operation operation =
            column.getType().getBasic() == TypeSpec.Basic.COUNTER
                ? ValueModifier.Operation.INCREMENT
                : ValueModifier.Operation.SET;
        assignments.add(
            ValueModifier.of(
                ValueModifier.Target.column(column.getName()),
                operation,
                Term.of(toGrpcValue(column.getType(), entry.getValue()))));
      }
    }
    return assignments;
  }

  private List<BuiltCondition> buildPrimaryKeyWhere(DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    List<BuiltCondition> relations = new ArrayList<>();

    for (Map.Entry<String, Object> entry : value.entrySet()) {
      ColumnSpec column = dbColumnGetter.getColumn(table, entry.getKey());
      if (isPrimaryKey(column)) {
        relations.add(
            BuiltCondition.of(
                column.getName(), Predicate.EQ, toGrpcValue(column.getType(), entry.getValue())));
      }
    }
    return relations;
  }

  private boolean isPrimaryKey(ColumnSpec column) {
    return table.getPartitionKeyColumnsList().contains(column)
        || table.getClusteringKeyColumnsList().contains(column);
  }
}
