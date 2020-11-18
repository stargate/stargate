package io.stargate.graphql.schema.fetchers.dml;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.DefaultRaw;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnComponentLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.lhs.LeftOperand;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultRelation;
import com.datastax.oss.driver.internal.querybuilder.term.TupleTerm;
import com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.TypedKeyValue;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class DmlFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final Table table;
  protected final NameMapping nameMapping;
  protected final CqlIdentifier keyspaceId;
  protected final CqlIdentifier tableId;

  protected DmlFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
    this.table = table;
    this.nameMapping = nameMapping;
    this.keyspaceId = CqlIdentifier.fromInternal(table.keyspace());
    this.tableId = CqlIdentifier.fromInternal(this.table.name());
  }

  protected List<Condition> buildIfConditions(
      Table table, Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<Condition> clause = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      Column column = getColumn(table, clauseEntry.getKey());
      CqlIdentifier columnId = CqlIdentifier.fromInternal(column.name());

      for (Map.Entry<String, Object> conditionMap : clauseEntry.getValue().entrySet()) {
        FilterOperator operator = FilterOperator.fromFieldName(conditionMap.getKey());
        clause.add(operator.buildCondition(column, conditionMap.getValue(), nameMapping));
      }
    }
    return clause;
  }

  protected List<Relation> buildFilterConditions(
      Table table, Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<Relation> relations = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      Column column = getColumn(table, clauseEntry.getKey());
      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        FilterOperator operator = FilterOperator.fromFieldName(condition.getKey());
        relations.add(operator.buildRelation(column, condition.getValue(), nameMapping));
      }
    }
    return relations;
  }

  protected List<Relation> buildClause(Table table, DataFetchingEnvironment environment) {
    if (environment.containsArgument("filter")) {
      Map<String, Map<String, Object>> columnList = environment.getArgument("filter");
      return buildFilterConditions(table, columnList);
    } else {
      Map<String, Object> value = environment.getArgument("value");
      List<Relation> relations = new ArrayList<>();
      if (value == null) return ImmutableList.of();

      for (Map.Entry<String, Object> entry : value.entrySet()) {
        Column column = getColumn(table, entry.getKey());
        relations.add(
            Relation.column(CqlIdentifier.fromInternal(column.name()))
                .isEqualTo(toCqlTerm(column, entry.getValue())));
      }
      return relations;
    }
  }

  protected List<TypedKeyValue> buildTypedKeyValueList(
      Table table, DataFetchingEnvironment environment) {
    return buildTypedKeyValueList(buildClause(table, environment));
  }

  protected List<TypedKeyValue> buildTypedKeyValueList(List<Relation> relations) {
    List<TypedKeyValue> typedKeyValues = new ArrayList<>();
    for (Relation rel : relations) {
      if (rel instanceof DefaultRelation) {
        LeftOperand leftOperand = ((DefaultRelation) rel).getLeftOperand();
        Term term = ((DefaultRelation) rel).getRightOperand();

        boolean queryByEntries = false;
        CqlIdentifier columnId;
        if (leftOperand instanceof ColumnComponentLeftOperand) {
          columnId = ((ColumnComponentLeftOperand) leftOperand).getColumnId();
          queryByEntries = true;
        } else {
          columnId = ((ColumnLeftOperand) leftOperand).getColumnId();
        }

        Column column = getColumn(table, columnId.asInternal());
        assert term != null;

        if (Objects.requireNonNull(column.type()).isUserDefined()) {
          // Null out the value for now since UDTs are not allowed for use with custom authorization
          typedKeyValues.add(
              new TypedKeyValue(
                  column.cqlName(), Objects.requireNonNull(column.type()).cqlDefinition(), null));
          continue;
        }

        if (term instanceof TupleTerm) {
          for (Term component : ((TupleTerm) term).getComponents()) {
            Object parsedObject =
                Objects.requireNonNull(column.type())
                    .codec()
                    .parse(((DefaultRaw) component).getRawExpression());

            typedKeyValues.add(
                new TypedKeyValue(
                    column.cqlName(),
                    Objects.requireNonNull(column.type()).cqlDefinition(),
                    parsedObject));
          }
        } else {
          typedKeyValues.add(
              typedKeyValueForDefaultRawTerm(
                  ((DefaultRelation) rel).getOperator(),
                  (DefaultRaw) term,
                  queryByEntries,
                  column));
        }
      }
    }

    return typedKeyValues;
  }

  private TypedKeyValue typedKeyValueForDefaultRawTerm(
      String operator, DefaultRaw term, boolean queryByEntries, Column column) {
    Object parsedObject;
    if ("contains".equals(operator.trim().toLowerCase()) || queryByEntries) {
      if (column.ofTypeListOrSet()) {
        parsedObject =
            Objects.requireNonNull(column.type())
                .parameters()
                .get(0)
                .codec()
                .parse(term.getRawExpression());
      } else {
        parsedObject =
            Objects.requireNonNull(column.type())
                .parameters()
                .get(1)
                .codec()
                .parse(term.getRawExpression());
      }

    } else if ("contains key".equals(operator.trim().toLowerCase())) {
      parsedObject =
          Objects.requireNonNull(column.type())
              .parameters()
              .get(0)
              .codec()
              .parse(term.getRawExpression());
    } else {
      if (Objects.requireNonNull(column.type()).isUserDefined()) {
        // Null out the value for now since UDTs are not allowed for use with custom authorization
        parsedObject = null;
      } else {
        parsedObject = Objects.requireNonNull(column.type()).codec().parse(term.getRawExpression());
      }
    }
    return new TypedKeyValue(
        column.cqlName(), Objects.requireNonNull(column.type()).cqlDefinition(), parsedObject);
  }

  protected CqlIdentifier getDBColumnName(Table table, String fieldName) {
    Column column = getColumn(table, fieldName);
    if (column == null) {
      return null;
    }
    return CqlIdentifier.fromInternal(column.name());
  }

  protected Column getColumn(Table table, String fieldName) {
    String columnName = nameMapping.getCqlName(table, fieldName);
    return table.column(columnName);
  }

  protected Term toCqlTerm(Column column, Object value) {
    return DataTypeMapping.toCqlTerm(column.type(), value, nameMapping);
  }
}
