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

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AllKeyspacesFetcher extends CassandraFetcher<List<Map<String, Object>>> {

  public AllKeyspacesFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  protected List<Map<String, Object>> get(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception {
    HTTPAwareContextImpl httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    List<Map<String, Object>> keyspaces =
        KeyspaceFormatter.formatResult(dataStore.schema().keyspaces(), environment);
    for (Map<String, Object> keyspace : keyspaces) {
      String keyspaceName = (String) keyspace.get("name");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> tables = (List<Map<String, Object>>) keyspace.get("tables");
      authorizationService.authorizeSchemaRead(
          token,
          Collections.singletonList(keyspaceName),
          tables.stream().map(t -> (String) t.get("name")).collect(Collectors.toList()));
    }
    return keyspaces;
  }
}
