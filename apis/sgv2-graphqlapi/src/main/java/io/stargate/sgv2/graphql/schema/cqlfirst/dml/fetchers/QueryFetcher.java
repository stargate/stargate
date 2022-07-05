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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.bridge.grpc.BytesValues;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.aggregations.AggregationsFetcherSupport;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFetcher extends DmlFetcher<Map<String, Object>> {

  private final AggregationsFetcherSupport aggregationsFetcherSupport;

  public QueryFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
    this.aggregationsFetcherSupport = new AggregationsFetcherSupport(nameMapping, table);
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception {
    Query query = buildQuery(environment);

    Response response = context.getBridge().executeQuery(query);
    ResultSet resultSet = response.getResultSet();

    Map<String, Object> result = new HashMap<>();
    result.put(
        "values",
        resultSet.getRowsList().stream()
            .map(
                row -> {
                  Map<String, Object> columns =
                      DataTypeMapping.toGraphqlValue(
                          nameMapping, table, resultSet.getColumnsList(), row);
                  return aggregationsFetcherSupport.addAggregationResults(
                      columns, environment, row, resultSet.getColumnsList());
                })
            .collect(Collectors.toList()));

    if (resultSet.hasPagingState()) {
      result.put("pageState", BytesValues.toBase64(resultSet.getPagingState()));
    }

    return result;
  }

  private Query buildQuery(DataFetchingEnvironment environment) {
    Value limit = null;
    if (environment.containsArgument("options")) {
      Map<String, Object> options = environment.getArgument("options");
      Object limitObj = options == null ? null : options.get("limit");
      if (limitObj != null) {
        limit = Values.of((int) limitObj);
      }
    }

    return new QueryBuilder()
        .select()
        .column(buildQueryColumns(environment))
        .function(aggregationsFetcherSupport.buildAggregatedFunctions(environment))
        .from(keyspaceName, table.getName())
        .where(buildClause(table, environment))
        .limit(limit)
        .groupBy(buildGroupBy(environment))
        .orderBy(buildOrderBy(environment))
        .parameters(buildParameters(environment))
        .build();
  }

  private List<String> buildGroupBy(DataFetchingEnvironment environment) {
    if (!environment.containsArgument("groupBy")) {
      return Collections.emptyList();
    }
    ImmutableList.Builder<String> groupBys = ImmutableList.builder();
    Map<String, Boolean> input = environment.getArgument("groupBy");
    // Start from the table definition so that we add the clauses in PK declaration order
    for (ColumnSpec column : table.getPartitionKeyColumnsList()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (input.getOrDefault(graphqlName, false)) {
        groupBys.add(column.getName());
      }
    }
    for (ColumnSpec column : table.getClusteringKeyColumnsList()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (input.getOrDefault(graphqlName, false)) {
        groupBys.add(column.getName());
      }
    }
    return groupBys.build();
  }

  private Map<String, Column.Order> buildOrderBy(DataFetchingEnvironment environment) {
    if (!environment.containsArgument("orderBy")) {
      return Collections.emptyMap();
    }
    Map<String, Column.Order> orderBy = new LinkedHashMap<>();
    List<String> orderList = environment.getArgument("orderBy");
    for (String order : orderList) {
      int split = order.lastIndexOf("_");
      String column = order.substring(0, split);
      boolean desc = order.substring(split + 1).equals("DESC");
      orderBy.put(
          dbColumnGetter.getDBColumnName(table, column),
          desc ? Column.Order.DESC : Column.Order.ASC);
    }
    return orderBy;
  }

  private String[] buildQueryColumns(DataFetchingEnvironment environment) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return new String[0];
    }

    // While convoluted, it's technically valid to reference 'values' multiple times under
    // different aliases, for example:
    // {
    //   values { id }
    //   values2: values { first_name last_name }
    // }
    // We iterate all the occurrences and build the union of their subsets.
    Set<String> queryColumns = new LinkedHashSet<>();
    for (SelectedField valuesField : valuesFields) {
      for (SelectedField selectedField : valuesField.getSelectionSet().getFields()) {
        if ("__typename".equals(selectedField.getName())) {
          continue;
        }

        String column = dbColumnGetter.getDBColumnName(table, selectedField.getName());
        if (column != null) {
          queryColumns.add(column);
        }
      }
    }
    return queryColumns.toArray(new String[0]);
  }
}
