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
package io.stargate.graphql.schema.schemafirst.fetchers.admin;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import java.util.Collections;

public abstract class SchemaFetcher<ResultT> extends CassandraFetcher<ResultT> {
  public SchemaFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  protected void authorize(AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaRead(
        authenticationSubject,
        Collections.singletonList(SchemaSourceDao.KEYSPACE_NAME),
        Collections.singletonList(SchemaSourceDao.TABLE_NAME),
        SourceAPI.GRAPHQL);
  }

  protected String getNamespace(DataFetchingEnvironment environment, DataStore dataStore) {
    String namespace = environment.getArgument("namespace");
    if (!dataStore.schema().keyspaceNames().contains(namespace)) {
      throw new IllegalArgumentException(
          String.format("Namespace '%s' does not exist.", namespace));
    }
    return namespace;
  }
}
