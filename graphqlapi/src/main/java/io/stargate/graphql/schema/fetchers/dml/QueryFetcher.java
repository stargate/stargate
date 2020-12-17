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
package io.stargate.graphql.schema.fetchers.dml;

import com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.builder.ColumnOrder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryFetcher extends DmlFetcher<Map<String, Object>> {

  public QueryFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(table, nameMapping, persistence, authenticationService, authorizationService);
  }

  @Override
  protected Map<String, Object> get(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception {
    BoundQuery query = buildQuery(environment, dataStore);
    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    ResultSet resultSet =
        authorizationService.authorizedDataRead(
            () -> dataStore.execute(query).get(),
            token,
            table.keyspace(),
            table.name(),
            TypedKeyValue.forSelect((BoundSelect) query),
            SourceAPI.GRAPHQL);

    Map<String, Object> result = new HashMap<>();
    result.put(
        "values",
        resultSet.currentPageRows().stream()
            .map(row -> DataTypeMapping.toGraphQLValue(nameMapping, table, row))
            .collect(Collectors.toList()));

    ByteBuffer pageState = resultSet.getPagingState();
    if (pageState != null) {
      result.put("pageState", Base64.getEncoder().encodeToString(pageState.array()));
    }

    return result;
  }

  private BoundQuery buildQuery(DataFetchingEnvironment environment, DataStore dataStore) {
    Long limit = null;
    if (environment.containsArgument("options")) {
      Map<String, Object> options = environment.getArgument("options");
      Object limitObj = options == null ? null : options.get("limit");
      if (limitObj != null) {
        limit = (long) (int) limitObj;
      }
    }
    return dataStore
        .queryBuilder()
        .select()
        .column(buildQueryColumns(environment))
        .from(table.keyspace(), table.name())
        .where(buildClause(table, environment))
        .limit(limit)
        .orderBy(buildOrderBy(environment))
        .build()
        .bind();
  }

  private List<ColumnOrder> buildOrderBy(DataFetchingEnvironment environment) {
    if (environment.containsArgument("orderBy")) {
      List<ColumnOrder> orderBy = new ArrayList<>();
      List<String> orderList = environment.getArgument("orderBy");
      for (String order : orderList) {
        int split = order.lastIndexOf("_");
        String column = order.substring(0, split);
        boolean desc = order.substring(split + 1).equals("DESC");
        orderBy.add(ColumnOrder.of(getDBColumnName(table, column), desc ? Order.DESC : Order.ASC));
      }
      return orderBy;
    }
    return ImmutableList.of();
  }

  private List<Column> buildQueryColumns(DataFetchingEnvironment environment) {
    if (environment.getSelectionSet().contains("values")) {
      SelectedField field = environment.getSelectionSet().getField("values");
      List<Column> fields = new ArrayList<>();
      for (SelectedField selectedField : field.getSelectionSet().getFields()) {
        if ("__typename".equals(selectedField.getName())) {
          continue;
        }

        String column = getDBColumnName(table, selectedField.getName());
        if (column != null) {
          fields.add(Column.reference(column));
        }
      }
      return fields;
    }

    return ImmutableList.of();
  }
}
