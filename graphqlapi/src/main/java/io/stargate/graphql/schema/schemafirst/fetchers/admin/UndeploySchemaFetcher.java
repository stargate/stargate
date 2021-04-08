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
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import java.util.UUID;

public class UndeploySchemaFetcher extends CassandraFetcher<Boolean> {

  public UndeploySchemaFetcher(
      AuthorizationService authorizationService, DataStoreFactory dataStoreFactory) {
    super(authorizationService, dataStoreFactory);
  }

  @Override
  protected Boolean get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {

    String keyspaceName = environment.getArgument("keyspace");
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      throw new IllegalArgumentException(
          String.format("Keyspace '%s' does not exist.", keyspaceName));
    }

    authorizationService.authorizeSchemaWrite(
        authenticationSubject,
        keyspaceName,
        null,
        Scope.MODIFY,
        SourceAPI.GRAPHQL,
        ResourceKind.KEYSPACE);

    UUID expectedVersion = getExpectedVersion(environment);
    boolean force = environment.getArgument("force");

    new SchemaSourceDao(dataStore).undeploy(keyspaceName, expectedVersion, force);
    return true;
  }

  private UUID getExpectedVersion(DataFetchingEnvironment environment) {
    // Unlike deploy, the field is mandatory.
    try {
      return UUID.fromString(environment.getArgument("expectedVersion"));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid 'expectedVersion' value.");
    }
  }
}
