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
import io.stargate.graphql.schema.fetchers.CassandraFetcher;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AllKeyspacesFetcher extends CassandraFetcher<List<Map<String, Object>>> {

  public AllKeyspacesFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  protected CompletionStage<List<Map<String, Object>>> get(
      DataFetchingEnvironment environment, DataStore dataStore) {
    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    return CompletableFuture.completedFuture(
        KeyspaceFormatter.formatResult(
            dataStore.schema().keyspaces(), environment, authorizationService, token));
  }
}
