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
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateKeyspaceFetcher implements SchemaFetcher {

  private final Persistence<?, ?, ?> persistence;
  private final AuthenticationService authenticationService;

  public CreateKeyspaceFetcher(
      Persistence<?, ?, ?> persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  @Override
  public String getQuery(DataFetchingEnvironment dataFetchingEnvironment) {
    String keyspaceName = dataFetchingEnvironment.getArgument("name");
    CreateKeyspaceStart start = SchemaBuilder.createKeyspace(keyspaceName);
    boolean ifNotExists =
        dataFetchingEnvironment.getArgumentOrDefault("ifNotExists", Boolean.FALSE);
    if (ifNotExists) {
      start = start.ifNotExists();
    }
    List<Map<String, String>> replication = dataFetchingEnvironment.getArgument("replication");
    CreateKeyspace createKeyspace = start.withReplicationOptions(parseReplication(replication));
    boolean durableWrites =
        dataFetchingEnvironment.getArgumentOrDefault("durableWrites", Boolean.TRUE);
    createKeyspace = createKeyspace.withDurableWrites(durableWrites);
    return createKeyspace.asCql();
  }

  private Map<String, Object> parseReplication(List<Map<String, String>> graphqlOptions) {
    Map<String, Object> result = new HashMap<>();
    for (Map<String, String> graphqlOption : graphqlOptions) {
      result.put(graphqlOption.get("key"), graphqlOption.get("value"));
    }
    return result;
  }

  @Override
  public Object get(DataFetchingEnvironment environment) throws Exception {
    HTTPAwareContextImpl httpAwareContext = environment.getContext();

    String token = httpAwareContext.getAuthToken();
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = persistence.newQueryState(clientState);
    DataStore dataStore = persistence.newDataStore(queryState, null);

    dataStore.query(getQuery(environment)).get();
    return true;
  }
}
