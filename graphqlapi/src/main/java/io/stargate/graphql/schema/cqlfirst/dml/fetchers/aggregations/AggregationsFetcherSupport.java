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

import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.AVG;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.COUNT;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.FLOAT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.INT_FUNCTION;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.MAX;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.MIN;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.SupportedAggregationFunctions.SUM;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.DbColumnGetter;
import java.util.List;
import java.util.Map;
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
      DataFetchingEnvironment environment, QueryBuilder.QueryBuilder__41 queryBuilder) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return;
    }

    for (SelectedField valuesField : valuesFields) {
      for (SelectedField selectedField : getAllFieldsWithNameArg(valuesField)) {
        Map<String, Object> arguments = selectedField.getArguments();
        String functionName = getNameArgument(arguments);
        switch (functionName) {
          case COUNT:
            addCount(arguments, queryBuilder, selectedField);
            break;
          case AVG:
            addAvg(arguments, queryBuilder, selectedField);
            break;
          case MIN:
            addMin(arguments, queryBuilder, selectedField);
            break;
          case MAX:
            addMax(arguments, queryBuilder, selectedField);
            break;
          case SUM:
            addSum(arguments, queryBuilder, selectedField);
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
        if (selectedField.getName().equals(INT_FUNCTION)) {
          putResultValue(columns, row, selectedField, Row::getInt);
        }
        if (selectedField.getName().equals(FLOAT_FUNCTION)) {
          putResultValue(columns, row, selectedField, Row::getFloat);
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
      columns.put(alias, rowValueExtractor.apply(row, alias));
    }
    // generate aggregation column name and get the value
    else {
      String columnName = generateAggregationColumnName(selectedField);
      columns.put(columnName, rowValueExtractor.apply(row, columnName));
    }
  }

  // it returns C* generated aggregation column name
  // for example, for the name = count and arguments = ['a']
  // it will return: 'system.count(a)'
  private String generateAggregationColumnName(SelectedField selectedField) {
    Map<String, Object> arguments = selectedField.getArguments();
    String functionName = getNameArgument(arguments);
    List<String> args = getAndValidateArgs(arguments, functionName);
    return String.format("system.%s(%s)", functionName, args.get(0));
  }

  private void addAvg(
      Map<String, Object> arguments,
      QueryBuilder.QueryBuilder__41 queryBuilder,
      SelectedField selectedField) {
    List<String> args = getAndValidateArgs(arguments, AVG);
    String column = getAndValidateColumn(args);

    // todo after https://github.com/stargate/stargate/issues/907 is done
    String alias = selectedField.getAlias();
    if (alias != null) {
      // queryBuilder.avg(column, alias);
    } else {
      // queryBuilder.avg(column);
    }
  }

  private void addCount(
      Map<String, Object> arguments,
      QueryBuilder.QueryBuilder__41 queryBuilder,
      SelectedField selectedField) {
    List<String> args = getAndValidateArgs(arguments, COUNT);
    String column = getAndValidateColumn(args);

    // todo after https://github.com/stargate/stargate/issues/907 is done
    String alias = selectedField.getAlias();
    if (alias != null) {
      // queryBuilder.avg(column, alias);
    } else {
      // queryBuilder.avg(column);
    }
  }

  private void addMin(
      Map<String, Object> arguments,
      QueryBuilder.QueryBuilder__41 queryBuilder,
      SelectedField selectedField) {
    List<String> args = getAndValidateArgs(arguments, MIN);
    String column = getAndValidateColumn(args);

    // todo after https://github.com/stargate/stargate/issues/907 is done
    String alias = selectedField.getAlias();
    if (alias != null) {
      // queryBuilder.min(column, alias);
    } else {
      // queryBuilder.min(column);
    }
  }

  private void addMax(
      Map<String, Object> arguments,
      QueryBuilder.QueryBuilder__41 queryBuilder,
      SelectedField selectedField) {
    List<String> args = getAndValidateArgs(arguments, MAX);
    String column = getAndValidateColumn(args);

    // todo after https://github.com/stargate/stargate/issues/907 is done
    String alias = selectedField.getAlias();
    if (alias != null) {
      // queryBuilder.max(column, alias);
    } else {
      // queryBuilder.max(column);
    }
  }

  private void addSum(
      Map<String, Object> arguments,
      QueryBuilder.QueryBuilder__41 queryBuilder,
      SelectedField selectedField) {
    List<String> args = getAndValidateArgs(arguments, SUM);
    String column = getAndValidateColumn(args);

    // todo after https://github.com/stargate/stargate/issues/907 is done
    String alias = selectedField.getAlias();
    if (alias != null) {
      // queryBuilder.sum(column, alias);
    } else {
      // queryBuilder.sum(column);
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

  private List<String> getAndValidateArgs(Map<String, Object> arguments, String functionName) {
    @SuppressWarnings("unchecked")
    List<String> args = (List<String>) arguments.get("args");
    if (args.size() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "The %s function takes only one argument, " + "but more arguments: %s were provided.",
              functionName, args));
    }
    return args;
  }

  private String getNameArgument(Map<String, Object> arguments) {
    return (String) arguments.get("name");
  }

  private List<SelectedField> getAllFieldsWithNameArg(SelectedField valuesField) {
    // all aggregation functions MUST have the 'name' argument
    return valuesField.getSelectionSet().getFields().stream()
        .filter(
            v -> {
              Object name = v.getArguments().get("name");
              // the 'name' value must be a non-null String
              return name instanceof String;
            })
        .collect(Collectors.toList());
  }
}
