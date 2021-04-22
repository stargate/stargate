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
import static java.util.stream.Stream.concat;

import graphql.GraphQLException;
import graphql.language.Field;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BulkMutationFetcher
    extends DmlFetcher<CompletableFuture<List<Map<String, Object>>>> {
  protected BulkMutationFetcher(
      Table table,
      NameMapping nameMapping,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(table, nameMapping, authorizationService, dataStoreFactory);
  }

  @Override
  protected CompletableFuture<List<Map<String, Object>>> get(
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
    System.out.println("queries: " + queries);
    OperationDefinition operation = environment.getOperationDefinition();
    if (operation.getDirectives().stream().anyMatch(d -> d.getName().equals(ATOMIC_DIRECTIVE))
        && operation.getSelectionSet().getSelections().size() > 1) {
      System.out.println("Execute batch for queries: " + queries);
      return executeAsBatch(environment, dataStore, queries, buildException);
    }

    if (buildException != null) {
      CompletableFuture<List<Map<String, Object>>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return f;
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
              .thenApply(rs -> toMutationResult(rs, values.get(finalI))));
    }
    System.out.println("returning: " + results);
    return convert(results);
  }

  private CompletableFuture<List<Map<String, Object>>> executeAsBatch(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      List<BoundQuery> queries,
      Exception buildException) {
    System.out.println("environment: " + environment);
    int selections = environment.getOperationDefinition().getSelectionSet().getSelections().size();
    System.out.println("number of selections: " + selections);
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

    boolean isLastSelection = isLastSelection(environment);
    if (buildException != null) {
      batchContext.setExecutionResult(buildException);
    } else {
      batchContext.add(queries);
      if (isLastSelection) {
        System.out.println("executing N queries:" + batchContext.getQueries());
        // All the statements were added successfully and this is the last selection
        // Use the dataStore containing the options
        DataStore batchDataStore = batchContext.getDataStore().orElse(dataStore);
        batchContext.setExecutionResult(batchDataStore.batch(batchContext.getQueries()));
      }
    }

    List<Map<String, Object>> values = environment.getArgument("values");

    return batchContext.getExecutionFuture().thenApply(rs -> toListOfMutationResults(rs, values));
  }

  private boolean isLastSelection(DataFetchingEnvironment environment) {
    String currentSelectionName = environment.getExecutionStepInfo().getField().getName();
    List<Selection> selectionSet =
        environment.getOperationDefinition().getSelectionSet().getSelections();
    Selection<?> lastSelection = selectionSet.get(selectionSet.size() - 1);
    if (lastSelection instanceof Field) {
      String lastFieldName = ((Field) lastSelection).getName();
      return currentSelectionName.equals(lastFieldName);
    }
    return false;
  }

  public static <T> CompletableFuture<List<T>> convert(List<CompletableFuture<T>> futures) {
    return futures.stream()
        .map(f -> f.thenApply(Stream::of))
        .reduce((a, b) -> a.thenCompose(xs -> b.thenApply(ys -> concat(xs, ys))))
        .map(f -> f.thenApply(s -> s.collect(Collectors.toList())))
        .orElse(CompletableFuture.completedFuture(Collections.emptyList()));
  }

  protected abstract List<BoundQuery> buildQueries(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception;
}
