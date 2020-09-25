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
package io.stargate.graphql.fetchers;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CreateTableDataFetcher
    implements io.stargate.graphql.fetchers.SchemaFetcher, DataFetcher {
  private final Persistence persistence;
  private AuthenticationService authenticationService;

  public CreateTableDataFetcher(
      Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  @Override
  public Object get(DataFetchingEnvironment environment) throws Exception {
    HTTPAwareContextImpl httpAwareContext = environment.getContext();

    String token = httpAwareContext.getAuthToken();
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = persistence.newQueryState(clientState);
    DataStore dataStore = persistence.newDataStore(queryState, null);

    CompletableFuture<ResultSet> resultSetSingle = dataStore.query(getQuery(environment));
    resultSetSingle.get();
    return true;
  }

  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    CreateTableStart start =
        SchemaBuilder.createTable(
            (String) dataFetchingEnvironment.getArgument("keyspaceName"),
            (String) dataFetchingEnvironment.getArgument("tableName"));

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
      if (table != null) {
        table = table.withPartitionKey((String) key.get("name"), decodeType(key.get("type")));
      } else {
        table = start.withPartitionKey((String) key.get("name"), decodeType(key.get("type")));
      }
    }

    List<Map<String, Object>> clusteringKeys =
        dataFetchingEnvironment.getArgument("clusteringKeys");
    if (clusteringKeys != null) {
      for (Map<String, Object> key : clusteringKeys) {
        table = table.withClusteringColumn((String) key.get("name"), decodeType(key.get("type")));
      }
    }

    List<Map<String, Object>> values = dataFetchingEnvironment.getArgument("values");
    if (values != null) {
      for (Map<String, Object> key : values) {
        table = table.withColumn((String) key.get("name"), decodeType(key.get("type")));
      }
    }

    CreateTableWithOptions options = null;
    if (clusteringKeys != null) {
      for (Map<String, Object> key : clusteringKeys) {
        if (options == null) {
          options =
              table.withClusteringOrder(
                  (String) key.get("name"), decodeClusteringOrder((String) key.get("order")));
        } else {
          options =
              options.withClusteringOrder(
                  (String) key.get("name"), decodeClusteringOrder((String) key.get("order")));
        }
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
    return ClusteringOrder.valueOf(order);
  }
}
