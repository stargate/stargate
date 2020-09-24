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

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.List;
import java.util.Map;

public class AlterTableAddFetcher
    implements io.stargate.graphql.fetchers.SchemaFetcher, DataFetcher {
  private final Persistence persistence;
  private AuthenticationService authenticationService;

  public AlterTableAddFetcher(
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

    return dataStore.query(getQuery(environment)).thenApply(result -> true);
  }

  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    AlterTableStart start =
        SchemaBuilder.alterTable(
            (String) dataFetchingEnvironment.getArgument("keyspaceName"),
            (String) dataFetchingEnvironment.getArgument("tableName"));

    List<Map<String, Object>> toAdd = dataFetchingEnvironment.getArgument("toAdd");
    if (toAdd.isEmpty()) {
      // TODO see if we can enforce that through the schema instead
      throw new IllegalArgumentException("toAdd must contain at least one element");
    }
    AlterTableAddColumnEnd table = null;
    for (Map<String, Object> column : toAdd) {
      if (table != null) {
        table = table.addColumn((String) column.get("name"), decodeType(column.get("type")));
      } else {
        table = start.addColumn((String) column.get("name"), decodeType(column.get("type")));
      }
    }
    return table.build().getQuery();
  }
}
