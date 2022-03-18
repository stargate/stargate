package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.Int32Value;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.grpc.BytesValues;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.grpc.proto.Rows;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.schema.SchemaConstants;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DmlFetcher<ResultT> extends CassandraFetcher<ResultT> {

  private static final Logger LOG = LoggerFactory.getLogger(DmlFetcher.class);

  protected final String keyspaceName;
  protected final CqlTable table;
  protected final NameMapping nameMapping;
  protected final DbColumnGetter dbColumnGetter;

  protected DmlFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    this.keyspaceName = keyspaceName;
    this.table = table;
    this.nameMapping = nameMapping;
    this.dbColumnGetter = new DbColumnGetter(nameMapping);
  }

  protected QueryParameters buildParameters(DataFetchingEnvironment environment) {
    Map<String, Object> options = environment.getArgument("options");
    if (options == null) {
      return DEFAULT_PARAMETERS;
    }

    QueryParameters.Builder builder = DEFAULT_PARAMETERS.toBuilder();

    Object consistency = options.get("consistency");
    if (consistency != null) {
      builder.setConsistency(
          ConsistencyValue.newBuilder().setValue(Consistency.valueOf((String) consistency)));
    }

    Object serialConsistency = options.get("serialConsistency");
    if (serialConsistency != null) {
      builder.setSerialConsistency(
          ConsistencyValue.newBuilder().setValue(Consistency.valueOf((String) serialConsistency)));
    }

    Object pageSize = options.get("pageSize");
    if (pageSize != null) {
      builder.setPageSize(Int32Value.of((Integer) pageSize));
    }

    Object pageState = options.get("pageState");
    if (pageState != null) {
      builder.setPagingState(BytesValues.ofBase64((String) pageState));
    }

    return builder.build();
  }

  protected List<BuiltCondition> buildConditions(Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<BuiltCondition> where = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      ColumnSpec column = dbColumnGetter.getColumn(table, clauseEntry.getKey());
      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        FilterOperator operator = FilterOperator.fromFieldName(condition.getKey());
        where.add(operator.buildCondition(column, condition.getValue(), nameMapping));
      }
    }
    return where;
  }

  protected List<BuiltCondition> buildClause(CqlTable table, DataFetchingEnvironment environment) {
    if (environment.containsArgument("filter")) {
      Map<String, Map<String, Object>> columnList = environment.getArgument("filter");
      return buildConditions(columnList);
    } else {
      Map<String, Object> value = environment.getArgument("value");
      if (value == null) {
        return ImmutableList.of();
      }
      List<BuiltCondition> relations = new ArrayList<>();
      for (Map.Entry<String, Object> entry : value.entrySet()) {
        ColumnSpec column = dbColumnGetter.getColumn(table, entry.getKey());
        Value whereValue = toGrpcValue(column.getType(), entry.getValue());
        relations.add(BuiltCondition.of(column.getName(), Predicate.EQ, whereValue));
      }
      return relations;
    }
  }

  protected boolean containsDirective(OperationDefinition operation, String directive) {
    return operation.getDirectives().stream().anyMatch(d -> d.getName().equals(directive));
  }

  protected CompletableFuture<Map<String, Object>> executeAsyncAccepted(
      Query query, Object originalValue, StargateGraphqlContext context) {
    context
        .getBridge()
        .executeQueryAsync(query)
        .whenComplete(
            (r, throwable) -> {
              if (throwable != null) {
                LOG.warn(
                    String.format(
                        "The query %s executed within the %s directive failed.",
                        query, SchemaConstants.ASYNC_DIRECTIVE),
                    throwable);
              }
            });
    // complete immediately with accepted=true without waiting for the result
    return CompletableFuture.completedFuture(toAcceptedMutationResult(originalValue));
  }

  /**
   * Builds the result for a mutation that was annotated with the {@code @async} directive. This is
   * fire-and-forget, so we simply reflect back the input value with an additional "accepted=true"
   * field.
   */
  protected ImmutableMap<String, Object> toAcceptedMutationResult(Object originalValue) {
    return ImmutableMap.of("value", originalValue, "accepted", true);
  }

  /** Same as {@link #toAcceptedMutationResult(Object)}, but for a list of bulk mutations. */
  protected CompletionStage<List<Map<String, Object>>> toListOfAcceptedMutationResults(
      List<Map<String, Object>> originalValues) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (Map<String, Object> originalValue : originalValues) {
      results.add(toAcceptedMutationResult(originalValue));
    }
    return CompletableFuture.completedFuture(results);
  }

  /** Builds the result for a regular non-async, non-batched mutation. */
  protected Map<String, Object> toMutationResult(Response response, Object originalValue) {
    ResultSet resultSet = response.getResultSet();
    if (resultSet.getRowsCount() == 0) {
      // Empty response so this was a non-conditional mutation, reflect back the input value since
      // it was successfully persisted.
      return ImmutableMap.of("value", originalValue, "applied", true);
    } else {
      return toMutationResultSingleRow(
          originalValue, resultSet.getRows(0), resultSet.getColumnsList());
    }
  }

  /**
   * Builds the result for a mutation that returned one row. Either the mutation was applied and we
   * can reflect back the input value, or the row contains the values that didn't match.
   */
  private ImmutableMap<String, Object> toMutationResultSingleRow(
      Object originalValue, Row row, List<ColumnSpec> columns) {
    boolean applied = isApplied(row, columns);
    Object finalValue =
        applied ? originalValue : DataTypeMapping.toGraphqlValue(nameMapping, table, columns, row);
    return ImmutableMap.of("value", finalValue, "applied", applied);
  }

  /**
   * Builds the result for a mutation that was annotated with the {@code @atomic} directive. As a
   * result, it was executed as part of a CQL batch with other mutations.
   */
  protected Map<String, Object> toBatchResult(
      Response response, Map<String, Object> originalValue) {
    ResultSet resultSet = response.getResultSet();
    if (isApplied(resultSet)) {
      // Simple case: the CQL batch was applied, the original value of each mutation was succesfully
      // persisted.
      return ImmutableMap.of("value", originalValue, "applied", true);
    } else {
      return toUnappliedBatchResult(resultSet, originalValue);
    }
  }

  /** Same as {@link #toBatchResult(Response, Map)}, but for a list of bulk mutations. */
  protected List<Map<String, Object>> toBatchResults(
      Response response, List<Map<String, Object>> originalValues) {
    ResultSet resultSet = response.getResultSet();
    boolean applied = isApplied(resultSet);
    List<Map<String, Object>> results = new ArrayList<>();
    for (Map<String, Object> originalValue : originalValues) {
      results.add(
          applied
              ? ImmutableMap.of("value", originalValue, "applied", true)
              : toUnappliedBatchResult(resultSet, originalValue));
    }
    return results;
  }

  /**
   * The mutation was executed as part of a CQL batch that failed as a whole. If this particular
   * mutation was responsible for the failure, the response will contain a corresponding row,
   * otherwise we'll just output "applied=false" without any more details.
   */
  private Map<String, Object> toUnappliedBatchResult(
      ResultSet resultSet, Map<String, Object> originalValue) {
    Map<String, Value> primaryKey =
        Streams.concat(
                table.getPartitionKeyColumnsList().stream(),
                table.getClusteringKeyColumnsList().stream())
            .collect(
                Collectors.toMap(
                    ColumnSpec::getName,
                    c ->
                        toGrpcValue(
                            c.getType(), originalValue.get(nameMapping.getGraphqlName(table, c)))));
    List<ColumnSpec> columns = resultSet.getColumnsList();
    return resultSet.getRowsList().stream()
        .filter(row -> matches(row, primaryKey, columns))
        .findFirst()
        .map(
            row ->
                ImmutableMap.of(
                    "applied",
                    false,
                    "value",
                    DataTypeMapping.toGraphqlValue(nameMapping, table, columns, row)))
        .orElse(ImmutableMap.of("applied", false));
  }

  private boolean matches(Row row, Map<String, Value> primaryKey, List<ColumnSpec> columns) {
    for (Map.Entry<String, Value> entry : primaryKey.entrySet()) {
      String name = entry.getKey();
      Value value = entry.getValue();
      int i = Rows.firstIndexOf(name, columns);
      if (i < 0 || !value.equals(row.getValues(i))) {
        return false;
      }
    }
    return true;
  }

  protected Value toGrpcValue(QueryOuterClass.TypeSpec type, Object value) {
    return DataTypeMapping.toGrpcValue(type, value, nameMapping);
  }

  protected Integer getTtl(DataFetchingEnvironment environment) {
    Map<String, Object> options = environment.getArgument("options");
    return options == null ? null : (Integer) options.get("ttl");
  }
}
