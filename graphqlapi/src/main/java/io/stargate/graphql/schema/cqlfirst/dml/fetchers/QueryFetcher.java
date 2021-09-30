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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.builder.ColumnOrder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations.AggregationsFetcherSupport;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFetcher extends DmlFetcher<Map<String, Object>> {

  private final AggregationsFetcherSupport aggregationsFetcherSupport;

  public QueryFetcher(Table table, NameMapping nameMapping) {
    super(table, nameMapping);
    this.aggregationsFetcherSupport = new AggregationsFetcherSupport(nameMapping, table);
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception {
    BoundQuery query = buildQuery(environment, context.getDataStore());

    ResultSet resultSet =
        context
            .getAuthorizationService()
            .authorizedDataRead(
                () ->
                    context.getDataStore().execute(query, __ -> buildParameters(environment)).get(),
                context.getSubject(),
                table.keyspace(),
                table.name(),
                TypedKeyValue.forSelect((BoundSelect) query),
                SourceAPI.GRAPHQL);

    Map<String, Object> result = new HashMap<>();
    result.put(
        "values",
        resultSet.currentPageRows().stream()
            .map(
                row -> {
                  Map<String, Object> columns =
                      DataTypeMapping.toGraphQLValue(nameMapping, table, row);
                  return aggregationsFetcherSupport.addAggregationResults(
                      columns, environment, row);
                })
            .collect(Collectors.toList()));

    ByteBuffer pageState = resultSet.getPagingState();
    if (pageState != null) {
      result.put("pageState", ByteBufferUtils.toBase64(pageState));
    }

    return result;
  }

  private BoundQuery buildQuery(DataFetchingEnvironment environment, DataStore dataStore) {
    Integer limit = null;
    if (environment.containsArgument("options")) {
      Map<String, Object> options = environment.getArgument("options");
      Object limitObj = options == null ? null : options.get("limit");
      if (limitObj != null) {
        limit = (int) limitObj;
      }
    }

    return dataStore
        .queryBuilder()
        .select()
        .column(buildQueryColumns(environment))
        .function(aggregationsFetcherSupport.buildAggregatedFunctions(environment))
        .from(table.keyspace(), table.name())
        .where(buildClause(table, environment))
        .limit(limit)
        .groupBy(buildGroupBy(environment))
        .orderBy(buildOrderBy(environment))
        .build()
        .bind();
  }

  private List<Column> buildGroupBy(DataFetchingEnvironment environment) {
    if (!environment.containsArgument("groupBy")) {
      return Collections.emptyList();
    }
    ImmutableList.Builder<Column> groupBys = ImmutableList.builder();
    Map<String, Boolean> input = environment.getArgument("groupBy");
    // Start from the table definition so that we add the clauses in PK declaration order
    for (Column column : table.primaryKeyColumns()) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (input.getOrDefault(graphqlName, false)) {
        groupBys.add(column);
      }
    }
    return groupBys.build();
  }

  private List<ColumnOrder> buildOrderBy(DataFetchingEnvironment environment) {
    if (environment.containsArgument("orderBy")) {
      List<ColumnOrder> orderBy = new ArrayList<>();
      List<String> orderList = environment.getArgument("orderBy");
      for (String order : orderList) {
        int split = order.lastIndexOf("_");
        String column = order.substring(0, split);
        boolean desc = order.substring(split + 1).equals("DESC");
        orderBy.add(
            ColumnOrder.of(
                dbColumnGetter.getDBColumnName(table, column), desc ? Order.DESC : Order.ASC));
      }
      return orderBy;
    }
    return ImmutableList.of();
  }

  private List<Column> buildQueryColumns(DataFetchingEnvironment environment) {
    List<SelectedField> valuesFields = environment.getSelectionSet().getFields("values");
    if (valuesFields.isEmpty()) {
      return ImmutableList.of();
    }

    // While convoluted, it's technically valid to reference 'values' multiple times under
    // different aliases, for example:
    // {
    //   values { id }
    //   values2: values { first_name last_name }
    // }
    // We iterate all the occurrences and build the union of their subsets.
    Set<Column> queryColumns = new LinkedHashSet<>();
    for (SelectedField valuesField : valuesFields) {
      for (SelectedField selectedField : valuesField.getSelectionSet().getFields()) {
        if ("__typename".equals(selectedField.getName())) {
          continue;
        }

        String column = dbColumnGetter.getDBColumnName(table, selectedField.getName());
        if (column != null) {
          queryColumns.add(Column.reference(column));
        }
      }
    }
    return ImmutableList.copyOf(queryColumns);
  }
}
