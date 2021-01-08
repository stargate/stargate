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
package io.stargate.graphql.schema.schemafirst.fetchers;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DropNamespaceFetcher extends NamespaceFetcher<Map<String, Object>> {

  public DropNamespaceFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {

    String name = environment.getArgument("name");
    boolean ifExists = environment.getArgumentOrDefault("ifExists", Boolean.FALSE);

    authorizationService.authorizeSchemaWrite(
        authenticationSubject, name, null, Scope.DROP, SourceAPI.GRAPHQL);

    try {
      dataStore
          .execute(dataStore.queryBuilder().drop().keyspace(name).ifExists(ifExists).build().bind())
          .get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw ((Error) cause);
      } else if (cause instanceof Exception) {
        throw ((Exception) cause);
      }
    }

    return ImmutableMap.of("applied", true, "query", environment.getGraphQLSchema().getQueryType());
  }
}
