package io.stargate.graphql.schema.fetchers.dml;

import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.FilterOperator;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class DmlFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final Table table;
  protected final NameMapping nameMapping;

  protected DmlFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService) {
    super(persistence, authenticationService);
    this.table = table;
    this.nameMapping = nameMapping;
  }

  protected List<Condition> buildIfConditions(
      Table table, Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<Condition> clause = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      Column column = getColumn(table, clauseEntry.getKey());

      for (Map.Entry<String, Object> conditionMap : clauseEntry.getValue().entrySet()) {
        FilterOperator operator = FilterOperator.fromFieldName(conditionMap.getKey());
        Condition condition;
        if (operator != FilterOperator.IN) {
          condition =
              operator.buildCondition(column.name(), toCqlTerm(column, conditionMap.getValue()));
        } else {
          condition =
              operator.buildCondition(
                  column.name(), buildListLiterals(column, conditionMap.getValue()));
        }
        clause.add(condition);
      }
    }
    return clause;
  }

  private List<Term> buildListLiterals(Column column, Object o) {
    if (o instanceof Collection<?>) {
      Collection<?> values = (Collection<?>) o;
      return values.stream().map(item -> toCqlTerm(column, item)).collect(Collectors.toList());
    }

    return Collections.singletonList(toCqlTerm(column, o));
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
        Relation relation;
        FilterOperator operator = FilterOperator.fromFieldName(condition.getKey());
        if (operator == FilterOperator.IN) {
          relation =
              operator.buildRelation(
                  column.name(), buildListLiterals(column, condition.getValue()));
        } else if (operator == FilterOperator.CONTAINS) {
          relation =
              operator.buildRelation(column.name(), toCqlElementTerm(column, condition.getValue()));
        } else if (condition.getKey().equals("containsKey")) {
          relation =
              operator.buildRelation(column.name(), toCqlKeyTerm(column, condition.getValue()));
        } else if (condition.getKey().equals("containsEntry")) {
          Column.ColumnType mapType = column.type();
          assert mapType != null && mapType.isMap();
          Map<String, Object> entry = (Map<String, Object>) condition.getValue();
          Column.ColumnType keyType = mapType.parameters().get(0);
          Term keyTerm = toCqlTerm(keyType, entry.get("key"));
          Column.ColumnType valueType = mapType.parameters().get(1);
          Term valueTerm = toCqlTerm(valueType, entry.get("value"));
          relation = operator.buildRelation(column.name(), keyTerm, valueTerm);
        } else {
          relation = operator.buildRelation(column.name(), toCqlTerm(column, condition.getValue()));
        }
        relations.add(relation);
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
            Relation.column(column.name()).isEqualTo(toCqlTerm(column, entry.getValue())));
      }
      return relations;
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

  protected Term toCqlTerm(Column column, Object value) {
    return toCqlTerm(column.type(), value);
  }

  private Term toCqlElementTerm(Column column, Object value) {
    Column.ColumnType collectionType = column.type();
    assert collectionType != null && collectionType.isCollection();
    Column.ColumnType elementType = collectionType.parameters().get(collectionType.isMap() ? 1 : 0);
    return toCqlTerm(elementType, value);
  }

  private Term toCqlKeyTerm(Column column, Object value) {
    Column.ColumnType mapType = column.type();
    assert mapType != null && mapType.isMap();
    Column.ColumnType keyType = mapType.parameters().get(0);
    return toCqlTerm(keyType, value);
  }

  private Term toCqlTerm(Column.ColumnType type, Object value) {
    return DataTypeMapping.toCqlTerm(type, value, nameMapping);
  }
}
