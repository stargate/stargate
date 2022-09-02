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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.aggregations;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilderImpl;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.DbColumnGetter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AggregationsFetcherSupport {

  private final CqlTable table;
  private final DbColumnGetter dbColumnGetter;

  public AggregationsFetcherSupport(NameMapping nameMapping, CqlTable table) {
    this.table = table;
    this.dbColumnGetter = new DbColumnGetter(nameMapping);
  }

  public List<QueryBuilderImpl.FunctionCall> buildAggregatedFunctions(
      DataFetchingEnvironment environment) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return Collections.emptyList();
    }

    List<QueryBuilderImpl.FunctionCall> functionCalls = new ArrayList<>();
    for (SelectedField selectedField : extractAllFieldsAndDeduplicate(valuesFields)) {
      Map<String, Object> arguments = selectedField.getArguments();
      // if there is no SupportedAggregationFunction for arguments, ignore
      getSupportedFunction(arguments)
          .ifPresent(
              f -> {
                switch (f) {
                  case COUNT:
                    functionCalls.add(
                        createAggregationFunctionCall(
                            arguments,
                            QueryBuilderImpl.FunctionCall::count,
                            selectedField,
                            SupportedAggregationFunction.COUNT));
                    break;
                  case AVG:
                    functionCalls.add(
                        createAggregationFunctionCall(
                            arguments,
                            QueryBuilderImpl.FunctionCall::avg,
                            selectedField,
                            SupportedAggregationFunction.AVG));
                    break;
                  case MIN:
                    functionCalls.add(
                        createAggregationFunctionCall(
                            arguments,
                            QueryBuilderImpl.FunctionCall::min,
                            selectedField,
                            SupportedAggregationFunction.MIN));
                    break;
                  case MAX:
                    functionCalls.add(
                        createAggregationFunctionCall(
                            arguments,
                            QueryBuilderImpl.FunctionCall::max,
                            selectedField,
                            SupportedAggregationFunction.MAX));
                    break;
                  case SUM:
                    functionCalls.add(
                        createAggregationFunctionCall(
                            arguments,
                            QueryBuilderImpl.FunctionCall::sum,
                            selectedField,
                            SupportedAggregationFunction.SUM));
                    break;
                }
              });
    }
    return functionCalls;
  }

  public Map<String, Object> addAggregationResults(
      Map<String, Object> columns,
      DataFetchingEnvironment environment,
      Row row,
      List<ColumnSpec> columnSpecs) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return columns;
    }
    for (SelectedField selectedField : extractAllFieldsAndDeduplicate(valuesFields)) {
      // if there is no SupportedGraphqlFunction for arguments, ignore
      SupportedGraphqlFunction.valueOfIgnoreCase(selectedField.getName())
          .ifPresent(
              f ->
                  putResultValue(
                      columns, row, columnSpecs, selectedField, f.getRowValueExtractor()));
    }
    return columns;
  }

  private void putResultValue(
      Map<String, Object> columns,
      Row row,
      List<ColumnSpec> columnSpecs,
      SelectedField selectedField,
      SupportedGraphqlFunction.ValueExtractor rowValueExtractor) {
    String alias = selectedField.getAlias();
    // put the returned value as alias
    if (alias != null) {
      columns.put(selectedField.getName(), rowValueExtractor.extract(row, alias, columnSpecs));
    }
    // generate aggregation column name and get the value
    else {
      String columnName = generateAggregationColumnName(selectedField);
      columns.put(selectedField.getName(), rowValueExtractor.extract(row, columnName, columnSpecs));
    }
  }

  // it returns C* generated aggregation column name
  // for example, for the name = count and arguments = ['a']
  // it will return: 'system.count(a)'
  private String generateAggregationColumnName(SelectedField selectedField) {
    Map<String, Object> arguments = selectedField.getArguments();
    Optional<SupportedAggregationFunction> functionName = getSupportedFunction(arguments);
    if (!functionName.isPresent()) {
      throw new IllegalStateException(
          String.format("The function for arguments: %s does not exists.", arguments));
    }
    List<String> args = getAndValidateArgs(arguments, functionName.get());
    return String.format("system.%s(%s)", functionName.get().getName(), args.get(0));
  }

  private QueryBuilderImpl.FunctionCall createAggregationFunctionCall(
      Map<String, Object> arguments,
      BiFunction<String, String, QueryBuilderImpl.FunctionCall> addAggregation,
      SelectedField selectedField,
      SupportedAggregationFunction supportedAggregationFunction) {
    List<String> args = getAndValidateArgs(arguments, supportedAggregationFunction);
    String column = getAndValidateColumn(args, supportedAggregationFunction);
    String alias = selectedField.getAlias();
    return addAggregation.apply(column, alias);
  }

  private String getAndValidateColumn(
      List<String> args, SupportedAggregationFunction supportedAggregationFunction) {
    String column = dbColumnGetter.getDBColumnName(table, args.get(0));
    if (column == null) {
      throw new IllegalArgumentException(
          String.format(
              "The column name: %s provided for the %s function does not exists.",
              args.get(0), supportedAggregationFunction.getName()));
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
              "The %s function takes only one argument, but more arguments: %s were provided.",
              supportedAggregationFunction.getName(), args));
    }
    return args;
  }

  private Optional<SupportedAggregationFunction> getSupportedFunction(
      Map<String, Object> arguments) {
    return SupportedAggregationFunction.valueOfIgnoreCase((String) arguments.get("name"));
  }

  private Set<SelectedField> extractAllFieldsAndDeduplicate(List<SelectedField> valuesFields) {
    // extract all inner selections set, and deduplicate them
    return valuesFields.stream()
        .flatMap(v -> v.getSelectionSet().getFields().stream())
        .collect(Collectors.toSet());
  }
}
