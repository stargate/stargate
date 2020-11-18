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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    String statement = buildQuery(environment);
    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    ResultSet resultSet =
        authorizationService.authorizedDataRead(
            () -> dataStore.query(statement).get(),
            token,
            buildTypedKeyValueList(table, environment));

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

  private String buildQuery(DataFetchingEnvironment environment) {
    Select select =
        QueryBuilder.selectFrom(keyspaceId, tableId)
            .columnsIds(buildQueryColumns(environment))
            .where(buildClause(this.table, environment))
            .orderByIds(buildOrderBy(environment));

    Map<String, Object> options = environment.getArgument("options");
    if (options != null) {
      Object limit = options.get("limit");
      if (limit != null) {
        select = select.limit((Integer) limit);
      }
    }

    return select.asCql();
  }

  private Map<CqlIdentifier, ClusteringOrder> buildOrderBy(DataFetchingEnvironment environment) {
    if (environment.containsArgument("orderBy")) {
      Map<CqlIdentifier, ClusteringOrder> orderMap = new LinkedHashMap<>();
      List<String> orderList = environment.getArgument("orderBy");
      for (String order : orderList) {
        int split = order.lastIndexOf("_");
        String column = order.substring(0, split);
        boolean desc = order.substring(split + 1).equals("DESC");
        orderMap.put(
            getDBColumnName(table, column), desc ? ClusteringOrder.DESC : ClusteringOrder.ASC);
      }
      return orderMap;
    }

    return ImmutableMap.of();
  }

  private List<CqlIdentifier> buildQueryColumns(DataFetchingEnvironment environment) {
    if (environment.getSelectionSet().contains("values")) {
      SelectedField field = environment.getSelectionSet().getField("values");
      List<CqlIdentifier> fields = new ArrayList<>();
      for (SelectedField selectedField : field.getSelectionSet().getFields()) {
        if ("__typename".equals(selectedField.getName())) {
          continue;
        }

        CqlIdentifier column = getDBColumnName(table, selectedField.getName());
        if (column != null) {
          fields.add(column);
        }
      }
      return fields;
    }

    return ImmutableList.of();
  }
}
