package io.stargate.graphql.schema.fetchers.dml;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        switch (condition.getKey()) {
          case "eq":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isEqualTo(literal(condition.getValue())));
            break;
          case "notEq":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isNotEqualTo(literal(condition.getValue())));
            break;
          case "gt":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isGreaterThan(literal(condition.getValue())));
            break;
          case "gte":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isGreaterThanOrEqualTo(literal(condition.getValue())));
            break;
          case "lt":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isLessThan(literal(condition.getValue())));
            break;
          case "lte":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isLessThanOrEqualTo(literal(condition.getValue())));
            break;
          case "in":
            clause.add(
                Condition.column(getDBColumnName(table, clauseEntry.getKey()))
                    .in(buildListLiterals(condition.getValue())));
            break;
          default:
        }
      }
    }
    return clause;
  }

  protected List<Term> buildListLiterals(Object o) {
    List<Term> literals = new ArrayList<>();
    if (o instanceof List) {
      List<?> values = (List<?>) o;
      for (Object value : values) {
        literals.add(literal(value));
      }
    } else {
      literals.add(literal(o));
    }
    return literals;
  }

  protected String getDBColumnName(Table table, String column) {
    return nameMapping.getColumnName(table).inverse().get(column).name();
  }

  protected List<Relation> buildFilterConditions(
      Table table, Map<String, Map<String, Object>> columnList) {
    if (columnList == null) {
      return ImmutableList.of();
    }
    List<Relation> relations = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
      for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
        switch (condition.getKey()) {
          case "eq":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isEqualTo(literal(condition.getValue())));
            break;
          case "notEq":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isNotEqualTo(literal(condition.getValue())));
            break;
          case "gt":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isGreaterThan(literal(condition.getValue())));
            break;
          case "gte":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isGreaterThanOrEqualTo(literal(condition.getValue())));
            break;
          case "lt":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isLessThan(literal(condition.getValue())));
            break;
          case "lte":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .isLessThanOrEqualTo(literal(condition.getValue())));
            break;
          case "in":
            relations.add(
                Relation.column(getDBColumnName(table, clauseEntry.getKey()))
                    .in(buildListLiterals(condition.getValue())));
            break;
          default:
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
        relations.add(
            Relation.column(getDBColumnName(table, entry.getKey()))
                .isEqualTo(literal(entry.getValue())));
      }
      return relations;
    }
  }
}
