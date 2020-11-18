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
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.SchemaEntity;
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleKeyspaceFetcher extends CassandraFetcher<Map<String, Object>> {

  public SingleKeyspaceFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  protected Map<String, Object> get(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception {
    String keyspaceName = environment.getArgument("name");

    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      return null;
    }

    authorizationService.authorizeSchemaRead(
        token,
        Collections.singletonList(keyspaceName),
        keyspace.tables().stream().map(SchemaEntity::name).collect(Collectors.toList()));

    return KeyspaceFormatter.formatResult(keyspace, environment);
  }
}
