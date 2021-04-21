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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static io.stargate.graphql.schema.SchemaConstants.ATOMIC_DIRECTIVE;

import com.google.common.collect.ImmutableMap;
import graphql.GraphQLException;
import graphql.language.*;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public abstract class BulkMutationFetcher
    extends DmlFetcher<List<CompletableFuture<Map<String, Object>>>> {
  protected BulkMutationFetcher(
      Table table,
      NameMapping nameMapping,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(table, nameMapping, authorizationService, dataStoreFactory);
  }

  @Override
  protected List<CompletableFuture<Map<String, Object>>> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject) {
    List<BoundQuery> queries = new ArrayList<>();
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildStatement() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      queries = buildQueries(environment, dataStore, authenticationSubject);
    } catch (Exception e) {
      buildException = e;
    }

    OperationDefinition operation = environment.getOperationDefinition();
    if (operation.getDirectives().stream().anyMatch(d -> d.getName().equals(ATOMIC_DIRECTIVE))
        && operation.getSelectionSet().getSelections().size() > 1) {
      return Collections.singletonList(
          executeAsBatch(environment, dataStore, queries, buildException));
    }

    if (buildException != null) {
      CompletableFuture<Map<String, Object>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return Collections.singletonList(f);
    }

    List<Map<String, Object>> values = environment.getArgument("values");
    if (values.size() != queries.size()) {
      throw new IllegalStateException("Number of values to insert should match number of queries");
    }

    List<CompletableFuture<Map<String, Object>>> results = new ArrayList<>(values.size());
    for (int i = 0; i < queries.size(); i++) {
      // Execute as a single statement
      int finalI = i;
      results.add(
          dataStore
              .execute(queries.get(i))
              .thenApply(rs -> ImmutableMap.of("value", values.get(finalI))));
    }
    return results;
  }

  private CompletableFuture<Map<String, Object>> executeAsBatch(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      List<BoundQuery> queries,
      Exception buildException) {
    int sumOfAllQueriesForAllSelections =
        calculateNumberOfAllQueries(environment.getOperationDefinition());
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
    } else {
      for (BoundQuery query : queries) {
        if (batchContext.add(query) == sumOfAllQueriesForAllSelections) {
          // All the statements were added successfully
          // Use the dataStore containing the options
          DataStore batchDataStore = batchContext.getDataStore().orElse(dataStore);
          batchContext.setExecutionResult(batchDataStore.batch(batchContext.getQueries()));
        }
      }
    }
    return batchContext
        .getExecutionFuture()
        .thenApply(v -> ImmutableMap.of("values", environment.getArgument("values")));
  }

  private int calculateNumberOfAllQueries(OperationDefinition operationDefinition) {
    int sumOfQueriesForAllSelections = 0;
    for (Selection<?> selection : operationDefinition.getSelectionSet().getSelections()) {
      if (selection instanceof Field) {
        @SuppressWarnings("unchecked")
        Optional<Value<?>> value =
            ((Field) selection)
                .getArguments().stream()
                    .filter(arg -> arg.getName().equals("values"))
                    .findAny()
                    .map(Argument::getValue);
        // for non-bulk queries, one selection == one query
        if (!value.isPresent()) {
          sumOfQueriesForAllSelections += 1;
        } else if (value.get() instanceof ArrayValue) {
          sumOfQueriesForAllSelections += value.get().getChildren().size();
        }
      } else {
        sumOfQueriesForAllSelections += 1;
      }
    }
    return sumOfQueriesForAllSelections;
  }

  protected abstract List<BoundQuery> buildQueries(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception;
}
