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
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;

public class DropTableFetcher implements io.stargate.graphql.fetchers.SchemaFetcher, DataFetcher {
  private final Persistence persistence;
  private AuthenticationService authenticationService;

  public DropTableFetcher(Persistence persistence, AuthenticationService authenticationService) {
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

  @Override
  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    Drop drop =
        SchemaBuilder.dropTable(
            (String) dataFetchingEnvironment.getArgument("keyspaceName"),
            (String) dataFetchingEnvironment.getArgument("tableName"));

    Boolean ifExists = dataFetchingEnvironment.getArgument("ifExists");
    if (ifExists != null && ifExists) {
      return drop.ifExists().build().getQuery();
    }
    return drop.build().getQuery();
  }
}
