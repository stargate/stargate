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
import io.stargate.auth.AuthnzService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Table;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import io.stargate.graphql.schema.NameMapping;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class MutationFetcher extends DmlFetcher<CompletableFuture<Map<String, Object>>> {

  protected MutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthnzService authnzService) {
    super(table, nameMapping, persistence, authnzService);
  }

  @Override
  protected CompletableFuture<Map<String, Object>> get(
      DataFetchingEnvironment environment, DataStore dataStore) {
    String statement = null;
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildStatement() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      statement = buildStatement(environment, dataStore);
    } catch (Exception e) {
      buildException = e;
    }

    OperationDefinition operation = environment.getOperationDefinition();

    if (operation.getDirectives().stream().anyMatch(d -> d.getName().equals(ATOMIC_DIRECTIVE))
        && operation.getSelectionSet().getSelections().size() > 1) {
      // There are more than one mutation in @atomic operation
      return executeAsBatch(environment, dataStore, statement, buildException);
    }

    if (buildException != null) {
      CompletableFuture<Map<String, Object>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return f;
    }

    // Execute as a single statement
    return dataStore
        .query(statement)
        .thenApply(rs -> ImmutableMap.of("value", environment.getArgument("value")));
  }

  private CompletableFuture<Map<String, Object>> executeAsBatch(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      String statement,
      Exception buildException) {
    int selections = environment.getOperationDefinition().getSelectionSet().getSelections().size();
    HTTPAwareContextImpl context = environment.getContext();
    HTTPAwareContextImpl.BatchContext batchContext = context.getBatchContext();

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
    } else if (batchContext.add(statement) == selections) {
      // All the statements were added successfully
      // Use the dataStore containing the options
      DataStore batchDataStore = batchContext.getDataStore().orElse(dataStore);
      batchContext.setExecutionResult(batchDataStore.batch(batchContext.getStatements()));
    }

    return batchContext
        .getExecutionFuture()
        .thenApply(v -> ImmutableMap.of("value", environment.getArgument("value")));
  }

  protected abstract String buildStatement(
      DataFetchingEnvironment environment, DataStore dataStore);
}
