package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public abstract class DmlFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final Table table;
  protected final NameMapping nameMapping;

  protected DmlFetcher(
      Table table,
      NameMapping nameMapping,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authorizationService, dataStoreFactory);
    this.table = table;
    this.nameMapping = nameMapping;
  }

  @Override
  protected Parameters getDatastoreParameters(DataFetchingEnvironment environment) {
    Map<String, Object> options = environment.getArgument("options");
    if (options == null) {
      return DEFAULT_PARAMETERS;
    }

    ImmutableParameters.Builder builder = Parameters.builder().from(DEFAULT_PARAMETERS);

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
      Column column = getColumn(table, clauseEntry.getKey());
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
        Column column = getColumn(table, entry.getKey());
        Object whereValue = toDBValue(column.type(), entry.getValue());
        relations.add(BuiltCondition.of(column.name(), Predicate.EQ, whereValue));
      }
      return relations;
    }
  }

  protected Map<String, Object> toMutationResult(ResultSet resultSet, Object originalValue) {

    List<Row> rows = resultSet.currentPageRows();

    if (rows.isEmpty()) {
      // if we have no rows means that we got no information back from query execution, return
      // original value to the user and applied true to denote that query was accepted
      // not matter if the underlying data is not changed
      ImmutableMap<String, Object> m = ImmutableMap.of("value", originalValue, "applied", true);
      System.out.println("return if.m: " + m);
      return m;

    } else {
      // otherwise check what can we get from the results
      // mutation target only one row
      Row row = rows.iterator().next();
      boolean applied = row.getBoolean("[applied]");
      Map<String, Object> value = DataTypeMapping.toGraphQLValue(nameMapping, table, row);

      // if applied we can return the original value, otherwise use database state
      Object finalValue = applied ? originalValue : value;
      ImmutableMap<String, Object> m = ImmutableMap.of("value", finalValue, "applied", applied);
      System.out.println("return else.m: " + m);
      return m;
    }
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
