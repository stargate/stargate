package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static io.stargate.graphql.schema.SchemaConstants.ASYNC_DIRECTIVE;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DmlFetcher<ResultT> extends CassandraFetcher<ResultT> {
  private static final Logger log = LoggerFactory.getLogger(DmlFetcher.class);
  protected final Table table;
  protected final NameMapping nameMapping;
  protected final DbColumnGetter dbColumnGetter;

  protected DmlFetcher(Table table, NameMapping nameMapping) {
    this.table = table;
    this.nameMapping = nameMapping;
    this.dbColumnGetter = new DbColumnGetter(nameMapping);
  }

  protected Parameters buildParameters(DataFetchingEnvironment environment) {
    Map<String, Object> options = environment.getArgument("options");
    if (options == null) {
      return DEFAULT_PARAMETERS;
    }

    ImmutableParameters.Builder builder = DEFAULT_PARAMETERS.toBuilder();

    Object consistency = options.get("consistency");
    if (consistency != null) {
      builder.consistencyLevel(ConsistencyLevel.valueOf((String) consistency));
    }

    Object serialConsistency = options.get("serialConsistency");
    if (serialConsistency != null) {
      builder.serialConsistencyLevel(ConsistencyLevel.valueOf((String) serialConsistency));
    }

    Object pageSize = options.get("pageSize");
    if (pageSize != null) {
      builder.pageSize((Integer) pageSize);
    }

    Object pageState = options.get("pageState");
    if (pageState != null) {
      builder.pagingState(ByteBuffer.wrap(Base64.getDecoder().decode((String) pageState)));
    }

    return builder.build();
  }

  protected List<BuiltCondition> buildConditions(
      Table table, Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<BuiltCondition> where = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      Column column = dbColumnGetter.getColumn(table, clauseEntry.getKey());
      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        FilterOperator operator = FilterOperator.fromFieldName(condition.getKey());
        where.add(operator.buildCondition(column, condition.getValue(), nameMapping));
      }
    }
    return where;
  }

  protected List<BuiltCondition> buildClause(Table table, DataFetchingEnvironment environment) {
    if (environment.containsArgument("filter")) {
      Map<String, Map<String, Object>> columnList = environment.getArgument("filter");
      return buildConditions(table, columnList);
    } else {
      Map<String, Object> value = environment.getArgument("value");
      List<BuiltCondition> relations = new ArrayList<>();
      if (value == null) return ImmutableList.of();

      for (Map.Entry<String, Object> entry : value.entrySet()) {
        Column column = dbColumnGetter.getColumn(table, entry.getKey());
        Object whereValue = toDBValue(column.type(), entry.getValue());
        relations.add(BuiltCondition.of(column.name(), Predicate.EQ, whereValue));
      }
      return relations;
    }
  }

  protected List<Map<String, Object>> toBatchResults(
      List<Row> rows, List<Map<String, Object>> originalValues) {

    boolean applied = isAppliedBatch(rows);

    List<Map<String, Object>> results = new ArrayList<>();
    for (Map<String, Object> originalValue : originalValues) {
      results.add(
          applied
              ? toAppliedMutationResultWithOriginalValue(originalValue)
              : toUnappliedBatchResult(rows, originalValue));
    }
    return results;
  }

  protected Map<String, Object> toBatchResult(List<Row> rows, Map<String, Object> originalValue) {
    return isAppliedBatch(rows)
        ? toAppliedMutationResultWithOriginalValue(originalValue)
        : toUnappliedBatchResult(rows, originalValue);
  }

  private Map<String, Object> toUnappliedBatchResult(
      List<Row> rows, final Map<String, Object> originalValue) {
    Map<String, Object> primaryKey =
        table.primaryKeyColumns().stream()
            .collect(
                Collectors.toMap(
                    Column::name, column -> toDBValue(column, originalValue.get(column.name()))));
    return rows.stream()
        .filter(row -> matches(row, primaryKey))
        .findFirst()
        .map(row -> toMutationResultSingleRow(originalValue, row))
        .orElse(ImmutableMap.of("applied", false));
  }

  private boolean matches(Row row, Map<String, Object> primaryKey) {
    for (Map.Entry<String, Object> entry : primaryKey.entrySet()) {
      String name = entry.getKey();
      if (row.columns().stream().noneMatch(c -> name.equals(c.name()))
          || !entry.getValue().equals(row.getObject(name))) {
        return false;
      }
    }
    return true;
  }

  protected CompletableFuture<List<Map<String, Object>>> toListOfMutationResultsAccepted(
      List<Map<String, Object>> originalValues) {
    List<Map<String, Object>> results = new ArrayList<>();
    for (Map<String, Object> originalValue : originalValues) {
      results.add(toAcceptedMutationResultWithOriginalValue(originalValue));
    }
    return CompletableFuture.completedFuture(results);
  }

  protected Map<String, Object> toMutationResult(ResultSet resultSet, Object originalValue) {

    List<Row> rows = resultSet.currentPageRows();

    if (rows.isEmpty()) {
      // if we have no rows means that we got no information back from query execution, return
      // original value to the user and applied true to denote that query was accepted
      // not matter if the underlying data is not changed
      return toAppliedMutationResultWithOriginalValue(originalValue);
    } else {
      // otherwise check what can we get from the results
      // mutation target only one row
      Row row = rows.iterator().next();
      return toMutationResultSingleRow(originalValue, row);
    }
  }

  private ImmutableMap<String, Object> toAppliedMutationResultWithOriginalValue(
      Object originalValue) {
    return ImmutableMap.of("value", originalValue, "applied", true);
  }

  protected ImmutableMap<String, Object> toAcceptedMutationResultWithOriginalValue(
      Object originalValue) {
    return ImmutableMap.of("value", originalValue, "accepted", true);
  }

  private ImmutableMap<String, Object> toMutationResultSingleRow(Object originalValue, Row row) {
    boolean applied = row.getBoolean("[applied]");

    // if applied we can return the original value, otherwise use database state
    Object finalValue =
        applied ? originalValue : DataTypeMapping.toGraphQLValue(nameMapping, table, row);
    return ImmutableMap.of("value", finalValue, "applied", applied);
  }

  protected boolean containsDirective(OperationDefinition operation, String directive) {
    return operation.getDirectives().stream().anyMatch(d -> d.getName().equals(directive));
  }

  /**
   * Executes the query in an async way in a fire and forget fashion. Completes immediately with
   * accepted=true without waiting for the result.
   */
  protected CompletableFuture<Map<String, Object>> executeAsyncAccepted(
      BoundQuery query,
      Object originalValue,
      UnaryOperator<Parameters> parameters,
      StargateGraphqlContext context) {
    context
        .getDataStore()
        .execute(query, parameters)
        .whenComplete(
            (r, throwable) -> {
              if (throwable != null) {
                log.warn(
                    String.format(
                        "The query %s executed within the %s directive failed.",
                        query, ASYNC_DIRECTIVE),
                    throwable);
              }
            });
    // complete immediately with accepted=true without waiting for the result
    return CompletableFuture.completedFuture(
        toAcceptedMutationResultWithOriginalValue(originalValue));
  }

  protected String getDBColumnName(Table table, String fieldName) {
    Column column = getColumn(table, fieldName);
    if (column == null) {
      return null;
    }
    return column.name();
  }

  protected Column getColumn(Table table, String fieldName) {
    String columnName = nameMapping.getCqlName(table, fieldName);
    return table.column(columnName);
  }

  protected Object toDBValue(Column column, Object value) {
    return toDBValue(column.type(), value);
  }

  private Object toDBValue(ColumnType type, Object value) {
    return DataTypeMapping.toDBValue(type, value, nameMapping);
  }
}
