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

import static io.stargate.graphql.schema.SchemaConstants.ASYNC_DIRECTIVE;
import static io.stargate.graphql.schema.SchemaConstants.ATOMIC_DIRECTIVE;
import static java.util.stream.Stream.concat;

import graphql.GraphQLException;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.db.Parameters;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BulkMutationFetcher
    extends DmlFetcher<CompletableFuture<List<Map<String, Object>>>> {
  protected BulkMutationFetcher(Table table, NameMapping nameMapping) {
    super(table, nameMapping);
  }

  @Override
  protected CompletableFuture<List<Map<String, Object>>> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    List<BoundQuery> queries = new ArrayList<>();
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildStatement() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      queries = buildQueries(environment, context);
    } catch (Exception e) {
      buildException = e;
    }
    OperationDefinition operation = environment.getOperationDefinition();

    if (containsDirective(operation, ATOMIC_DIRECTIVE)
        && (operation.getSelectionSet().getSelections().size() > 1 || queries.size() > 1)) {
      return executeAsPartOfBatch(environment, queries, buildException, operation);
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
    Parameters parameters = buildParameters(environment);
    for (int i = 0; i < queries.size(); i++) {
      int finalI = i;
      // Execute as a single statement
      if (containsDirective(operation, ASYNC_DIRECTIVE)) {
        results.add(
            executeAsyncAccepted(queries.get(i), values.get(finalI), __ -> parameters, context));
      } else {
        results.add(
            context
                .getDataStore()
                .execute(queries.get(i), __ -> parameters)
                .thenApply(rs -> toMutationResult(rs, values.get(finalI))));
      }
    }
    return convert(results);
  }

  private CompletableFuture<List<Map<String, Object>>> executeAsPartOfBatch(
      DataFetchingEnvironment environment,
      List<BoundQuery> queries,
      Exception buildException,
      OperationDefinition operation) {
    int selections = environment.getOperationDefinition().getSelectionSet().getSelections().size();
    StargateGraphqlContext context = environment.getContext();
    StargateGraphqlContext.BatchContext batchContext = context.getBatchContext();

    if (environment.getArgument("options") != null
        && !batchContext.setParameters(buildParameters(environment))) {
      buildException =
          new GraphQLException("options can only de defined once in an @atomic mutation selection");
    }

    if (buildException != null) {
      batchContext.setExecutionResult(buildException);
    } else if (batchContext.add(queries) == selections) {
      // All the statements were added successfully and this is the last selection
      batchContext.setExecutionResult(
          context
              .getDataStore()
              .batch(batchContext.getQueries(), __ -> batchContext.getParameters()));
    }

    List<Map<String, Object>> values = environment.getArgument("values");

    if (containsDirective(operation, ASYNC_DIRECTIVE)) {
      // does not wait for the batch execution result, return the accepted response for all values
      // immediately
      return toListOfMutationResultsAccepted(values);
    } else {
      return batchContext.getExecutionFuture().thenApply(rows -> toBatchResults(rows, values));
    }
  }

  public static <T> CompletableFuture<List<T>> convert(List<CompletableFuture<T>> futures) {
    return futures.stream()
        .map(f -> f.thenApply(Stream::of))
        .reduce((a, b) -> a.thenCompose(xs -> b.thenApply(ys -> concat(xs, ys))))
        .map(f -> f.thenApply(s -> s.collect(Collectors.toList())))
        .orElse(CompletableFuture.completedFuture(Collections.emptyList()));
  }

  protected abstract List<BoundQuery> buildQueries(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception;
}
