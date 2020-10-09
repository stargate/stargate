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
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class DmlFetcher extends CassandraFetcher<Map<String, Object>> {

  protected final Table table;
  protected final NameMapping nameMapping;

  protected DmlFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence<?, ?, ?> persistence,
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

        if (condition.getKey().equals("in")) {
          relations.add(
              Relation.column(column.name()).in(buildListLiterals(column, condition.getValue())));
        }
        Term dbValue = toCqlTerm(column, condition.getValue());

        switch (condition.getKey()) {
          case "eq":
            relations.add(Relation.column(column.name()).isEqualTo(dbValue));
            break;
          case "notEq":
            relations.add(Relation.column(column.name()).isNotEqualTo(dbValue));
            break;
          case "gt":
            relations.add(Relation.column(column.name()).isGreaterThan(dbValue));
            break;
          case "gte":
            relations.add(Relation.column(column.name()).isGreaterThanOrEqualTo(dbValue));
            break;
          case "lt":
            relations.add(Relation.column(column.name()).isLessThan(dbValue));
            break;
          case "lte":
            relations.add(Relation.column(column.name()).isLessThanOrEqualTo(dbValue));
            break;
          default:
            break;
        }
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
    return nameMapping.getColumnNames(table).inverse().get(fieldName);
  }

  protected Term toCqlTerm(Column column, Object value) {
    return DataTypeMapping.toCqlTerm(column.type(), value, nameMapping);
  }
}
