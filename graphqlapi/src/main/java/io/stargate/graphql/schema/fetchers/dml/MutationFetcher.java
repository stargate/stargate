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
package io.stargate.graphql.schema.fetchers.dml;

import static io.stargate.graphql.schema.SchemaConstants.ATOMIC_DIRECTIVE;

import com.google.common.collect.ImmutableMap;
import graphql.GraphQLException;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class MutationFetcher extends DmlFetcher<Map<String, Object>> {
  protected MutationFetcher(
      Table table,
      NameMapping nameMapping,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(table, nameMapping, authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected CompletableFuture<Map<String, Object>> get(
      DataFetchingEnvironment environment, DataStore dataStore) {
    BoundQuery query = null;
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildStatement() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      query = buildQuery(environment, dataStore);
    } catch (Exception e) {
      buildException = e;
    }

    OperationDefinition operation = environment.getOperationDefinition();

    if (operation.getDirectives().stream().anyMatch(d -> d.getName().equals(ATOMIC_DIRECTIVE))
        && operation.getSelectionSet().getSelections().size() > 1) {
      // There are more than one mutation in @atomic operation
      return executeAsBatch(environment, dataStore, query, buildException);
    }

    if (buildException != null) {
      CompletableFuture<Map<String, Object>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return f;
    }

    // Execute as a single statement
    return dataStore
        .execute(query)
        .thenApply(rs -> ImmutableMap.of("value", environment.getArgument("value")));
  }

  private CompletableFuture<Map<String, Object>> executeAsBatch(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      BoundQuery query,
      Exception buildException) {
    int selections = environment.getOperationDefinition().getSelectionSet().getSelections().size();
    HttpAwareContext context = environment.getContext();
    HttpAwareContext.BatchContext batchContext = context.getBatchContext();

    if (environment.getArgument("options") != null) {
      // Users should specify query options once in the batch
      boolean dataStoreAlreadySet = batchContext.setDataStore(dataStore);

      if (dataStoreAlreadySet) {
        // DataStore can be set at most once.
        // The instance that should be used should contain the user options (if any).
        buildException =
            new GraphQLException(
                "options can only de defined once in an @atomic mutation selection");
      }
    }

    if (buildException != null) {
      batchContext.setExecutionResult(buildException);
    } else if (batchContext.add(query) == selections) {
      // All the statements were added successfully
      // Use the dataStore containing the options
      DataStore batchDataStore = batchContext.getDataStore().orElse(dataStore);
      batchContext.setExecutionResult(batchDataStore.batch(batchContext.getQueries()));
    }

    return batchContext
        .getExecutionFuture()
        .thenApply(v -> ImmutableMap.of("value", environment.getArgument("value")));
  }

  protected abstract BoundQuery buildQuery(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception;

  protected Integer getTTL(DataFetchingEnvironment environment) {
    Integer ttl = null;
    if (environment.containsArgument("options") && environment.getArgument("options") != null) {
      Map<String, Object> options = environment.getArgument("options");
      if (options.containsKey("ttl") && options.get("ttl") != null) {
        ttl = (Integer) options.get("ttl");
      }
    }
    return ttl;
  }
}
