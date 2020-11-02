package io.stargate.graphql.schema.fetchers.dml;

import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthnzService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
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
      AuthnzService authnzService) {
    super(persistence, authnzService);
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

      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        if (condition.getKey().equals("in")) {
          clause.add(
              Condition.column(column.name()).in(buildListLiterals(column, condition.getValue())));
          continue;
        }

        Term dbValue = toCqlTerm(column, condition.getValue());
        switch (condition.getKey()) {
          case "eq":
            clause.add(Condition.column(column.name()).isEqualTo(dbValue));
            break;
          case "notEq":
            clause.add(Condition.column(column.name()).isNotEqualTo(dbValue));
            break;
          case "gt":
            clause.add(Condition.column(column.name()).isGreaterThan(dbValue));
            break;
          case "gte":
            clause.add(Condition.column(column.name()).isGreaterThanOrEqualTo(dbValue));
            break;
          case "lt":
            clause.add(Condition.column(column.name()).isLessThan(dbValue));
            break;
          case "lte":
            clause.add(Condition.column(column.name()).isLessThanOrEqualTo(dbValue));
            break;
          default:
            break;
        }
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

        ColumnRelationBuilder<Relation> relationStart = Relation.column(column.name());
        Relation relation;
        if (condition.getKey().equals("in")) {
          relation = relationStart.in(buildListLiterals(column, condition.getValue()));
        } else if (condition.getKey().equals("contains")) {
          relation = relationStart.contains(toCqlElementTerm(column, condition.getValue()));
        } else if (condition.getKey().equals("containsKey")) {
          relation = relationStart.containsKey(toCqlKeyTerm(column, condition.getValue()));
        } else if (condition.getKey().equals("containsEntry")) {
          Column.ColumnType mapType = column.type();
          assert mapType != null && mapType.isMap();
          Map<String, Object> entry = (Map<String, Object>) condition.getValue();
          Column.ColumnType keyType = mapType.parameters().get(0);
          Term keyTerm = toCqlTerm(keyType, entry.get("key"));
          Column.ColumnType valueType = mapType.parameters().get(1);
          Term valueTerm = toCqlTerm(valueType, entry.get("value"));
          relation = Relation.mapValue(column.name(), keyTerm).isEqualTo(valueTerm);
        } else {
          Term rightTerm = toCqlTerm(column, condition.getValue());
          switch (condition.getKey()) {
            case "eq":
              relation = relationStart.isEqualTo(rightTerm);
              break;
            case "notEq":
              relation = relationStart.isNotEqualTo(rightTerm);
              break;
            case "gt":
              relation = relationStart.isGreaterThan(rightTerm);
              break;
            case "gte":
              relation = relationStart.isGreaterThanOrEqualTo(rightTerm);
              break;
            case "lt":
              relation = relationStart.isLessThan(rightTerm);
              break;
            case "lte":
              relation = relationStart.isLessThanOrEqualTo(rightTerm);
              break;
            default:
              throw new IllegalStateException("Unsupported relation type " + condition.getKey());
          }
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
