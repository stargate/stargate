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
package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Resource;
import io.stargate.auth.SourceAPI;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.CassandraFetcher;
import java.util.Collections;

public class SingleKeyspaceFetcher extends CassandraFetcher<KeyspaceDto> {

  public SingleKeyspaceFetcher(
      AuthorizationService authorizationService, DataStoreFactory dataStoreFactory) {
    super(authorizationService, dataStoreFactory);
  }

  @Override
  protected KeyspaceDto get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {
    String keyspaceName = environment.getArgument("name");

    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      return null;
    }

    authorizationService.authorizeSchemaRead(
        authenticationSubject,
        Collections.singletonList(keyspace.name()),
        null,
        SourceAPI.GRAPHQL,
        Resource.KEYSPACE);

    return new KeyspaceDto(keyspace, authorizationService, authenticationSubject);
  }
}
