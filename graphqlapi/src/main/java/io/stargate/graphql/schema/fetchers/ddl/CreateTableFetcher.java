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
package io.stargate.graphql.schema.fetchers.ddl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.List;
import java.util.Map;

public class CreateTableFetcher extends DdlQueryFetcher {

  public CreateTableFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment)
      throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String tableName = dataFetchingEnvironment.getArgument("tableName");

    HTTPAwareContextImpl httpAwareContext = dataFetchingEnvironment.getContext();
    String token = httpAwareContext.getAuthToken();
    authorizationService.authorizeSchemaWrite(token, keyspaceName, tableName, Scope.CREATE);

    CreateTableStart start =
        SchemaBuilder.createTable(
            CqlIdentifier.fromInternal(keyspaceName), CqlIdentifier.fromInternal(tableName));

    Boolean ifNotExists = dataFetchingEnvironment.getArgument("ifNotExists");
    if (ifNotExists != null && ifNotExists) {
      start = start.ifNotExists();
    }

    CreateTable table = null;
    List<Map<String, Object>> partitionKeys = dataFetchingEnvironment.getArgument("partitionKeys");
    if (partitionKeys.isEmpty()) {
      // TODO see if we can enforce that through the schema instead
      throw new IllegalArgumentException("partitionKeys must contain at least one element");
    }
    for (Map<String, Object> key : partitionKeys) {
      table =
          (table == null ? start : table)
              .withPartitionKey(
                  CqlIdentifier.fromInternal((String) key.get("name")),
                  decodeType(key.get("type")));
    }

    List<Map<String, Object>> clusteringKeys =
        dataFetchingEnvironment.getArgument("clusteringKeys");
    if (clusteringKeys != null) {
      for (Map<String, Object> key : clusteringKeys) {
        table =
            table.withClusteringColumn(
                CqlIdentifier.fromInternal((String) key.get("name")), decodeType(key.get("type")));
      }
    }

    List<Map<String, Object>> values = dataFetchingEnvironment.getArgument("values");
    if (values != null) {
      for (Map<String, Object> key : values) {
        table =
            table.withColumn(
                CqlIdentifier.fromInternal((String) key.get("name")), decodeType(key.get("type")));
      }
    }

    CreateTableWithOptions options = null;
    if (clusteringKeys != null) {
      for (Map<String, Object> key : clusteringKeys) {
        options =
            (options == null ? table : options)
                .withClusteringOrder(
                    CqlIdentifier.fromInternal((String) key.get("name")),
                    decodeClusteringOrder((String) key.get("order")));
      }
    }
    String query;
    if (options != null) {
      query = options.build().getQuery();
    } else {
      query = table.build().getQuery();
    }

    return query;
  }

  private ClusteringOrder decodeClusteringOrder(String order) {
    if (order == null) {
      // Use the same default as CQL
      return ClusteringOrder.ASC;
    }
    return ClusteringOrder.valueOf(order);
  }
}
