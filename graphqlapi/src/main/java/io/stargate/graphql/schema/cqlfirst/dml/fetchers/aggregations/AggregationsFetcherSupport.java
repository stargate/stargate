/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations;

import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunction.AVG;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunction.COUNT;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunction.MAX;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunction.MIN;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunction.SUM;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.DbColumnGetter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AggregationsFetcherSupport {

  private final Table table;
  private final DbColumnGetter dbColumnGetter;

  public AggregationsFetcherSupport(NameMapping nameMapping, Table table) {
    this.table = table;
    this.dbColumnGetter = new DbColumnGetter(nameMapping);
  }

  public void addAggregationFunctions(
      DataFetchingEnvironment environment, QueryBuilder.QueryBuilder__20 queryBuilder) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return;
    }

    for (SelectedField valuesField : valuesFields) {
      for (SelectedField selectedField : getAllFieldsWithNameArg(valuesField)) {
        Map<String, Object> arguments = selectedField.getArguments();
        SupportedAggregationFunction functionName = getNameArgument(arguments);
        switch (functionName) {
          case COUNT:
            addAggregation(
                arguments,
                queryBuilder,
                QueryBuilder.QueryBuilder__20::count,
                selectedField,
                COUNT);
            break;
          case AVG:
            addAggregation(
                arguments, queryBuilder, QueryBuilder.QueryBuilder__20::avg, selectedField, AVG);
            break;
          case MIN:
            addAggregation(
                arguments, queryBuilder, QueryBuilder.QueryBuilder__20::min, selectedField, MIN);
            break;
          case MAX:
            addAggregation(
                arguments, queryBuilder, QueryBuilder.QueryBuilder__20::max, selectedField, MAX);
            break;
          case SUM:
            addAggregation(
                arguments, queryBuilder, QueryBuilder.QueryBuilder__20::sum, selectedField, SUM);
            break;
        }
      }
    }
  }

  public Map<String, Object> addAggregationResults(
      Map<String, Object> columns, DataFetchingEnvironment environment, Row row) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return columns;
    }
    for (SelectedField valuesField : valuesFields) {
      for (SelectedField selectedField : getAllFieldsWithNameArg(valuesField)) {
        switch (SupportedGraphqlFunction.valueOfIgnoreCase(selectedField.getName())) {
          case INT_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getInt);
            break;
          case DOUBLE_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getDouble);
            break;
          case BIGINT_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getLong);
            break;
          case DECIMAL_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getBigDecimal);
            break;
          case VARINT_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getBigInteger);
            break;
          case FLOAT_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getFloat);
            break;
          case SMALLINT_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getShort);
            break;
          case TINYINT_FUNCTION:
            putResultValue(columns, row, selectedField, Row::getByte);
            break;
        }
      }
    }
    return columns;
  }

  private void putResultValue(
      Map<String, Object> columns,
      Row row,
      SelectedField selectedField,
      BiFunction<Row, String, Object> rowValueExtractor) {
    String alias = selectedField.getAlias();
    // put the returned value as alias
    if (alias != null) {
      columns.put(selectedField.getName(), rowValueExtractor.apply(row, alias));
    }
    // generate aggregation column name and get the value
    else {
      String columnName = generateAggregationColumnName(selectedField);
      columns.put(selectedField.getName(), rowValueExtractor.apply(row, columnName));
    }
  }

  // it returns C* generated aggregation column name
  // for example, for the name = count and arguments = ['a']
  // it will return: 'system.count(a)'
  private String generateAggregationColumnName(SelectedField selectedField) {
    Map<String, Object> arguments = selectedField.getArguments();
    SupportedAggregationFunction functionName = getNameArgument(arguments);
    List<String> args = getAndValidateArgs(arguments, functionName);
    return String.format("system.%s(%s)", functionName.getName(), args.get(0));
  }

  private void addAggregation(
      Map<String, Object> arguments,
      QueryBuilder.QueryBuilder__20 queryBuilder,
      BiFunction<QueryBuilder.QueryBuilder__20, String, QueryBuilder.QueryBuilder__19>
          addAggregation,
      SelectedField selectedField,
      SupportedAggregationFunction supportedAggregationFunction) {
    List<String> args = getAndValidateArgs(arguments, supportedAggregationFunction);
    String column = getAndValidateColumn(args);

    String alias = selectedField.getAlias();
    QueryBuilder.QueryBuilder__19 withAggregation = addAggregation.apply(queryBuilder, column);
    if (alias != null) {
      withAggregation.as(alias);
    }
  }

  private String getAndValidateColumn(List<String> args) {
    String column = dbColumnGetter.getDBColumnName(table, args.get(0));
    if (column == null) {
      throw new IllegalArgumentException(
          String.format(
              "The column name: %s provided for the %s function does not exists.",
              args.get(0), COUNT));
    }
    return column;
  }

  private List<String> getAndValidateArgs(
      Map<String, Object> arguments, SupportedAggregationFunction supportedAggregationFunction) {
    @SuppressWarnings("unchecked")
    List<String> args = (List<String>) arguments.get("args");
    if (args.size() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "The %s function takes only one argument, " + "but more arguments: %s were provided.",
              supportedAggregationFunction.getName(), args));
    }
    return args;
  }

  private SupportedAggregationFunction getNameArgument(Map<String, Object> arguments) {
    return SupportedAggregationFunction.valueOfIgnoreCase((String) arguments.get("name"));
  }

  private Set<SelectedField> getAllFieldsWithNameArg(SelectedField valuesField) {
    // all aggregation functions MUST have the 'name' argument
    return valuesField.getSelectionSet().getFields().stream()
        .filter(
            v -> {
              Object name = v.getArguments().get("name");
              // the 'name' value must be a non-null String
              return name instanceof String;
            })
        .collect(Collectors.toSet());
  }
}
