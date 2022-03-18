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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static io.stargate.sgv2.graphql.schema.SchemaConstants.ASYNC_DIRECTIVE;
import static io.stargate.sgv2.graphql.schema.SchemaConstants.ATOMIC_DIRECTIVE;
import static java.util.stream.Stream.concat;

import graphql.GraphQLException;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.Schema;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BulkMutationFetcher
    extends DmlFetcher<CompletionStage<List<Map<String, Object>>>> {

  protected BulkMutationFetcher(
      String keyspaceName, Schema.CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected CompletionStage<List<Map<String, Object>>> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    List<Query> queries = new ArrayList<>();
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildQueries() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      queries = buildQueries(environment, context);
    } catch (Exception e) {
      buildException = e;
    }
    OperationDefinition operation = environment.getOperationDefinition();

    if (containsDirective(operation, ATOMIC_DIRECTIVE)
        && (operation.getSelectionSet().getSelections().size() > 1 || queries.size() > 1)) {
      return executeAsPartOfBatch(queries, buildException, operation, environment, context);
    }

    if (buildException != null) {
      CompletableFuture<List<Map<String, Object>>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return f;
    }

    List<Map<String, Object>> values = environment.getArgument("values");
    assert values.size() != queries.size(); // per the contract of buildQueries()

    List<CompletionStage<Map<String, Object>>> results = new ArrayList<>(values.size());
    QueryParameters parameters = buildParameters(environment);
    boolean isAsync = containsDirective(operation, ASYNC_DIRECTIVE);
    for (int i = 0; i < queries.size(); i++) {
      int finalI = i;
      // Execute as a single statement
      Query query = Query.newBuilder(queries.get(i)).setParameters(parameters).build();
      if (isAsync) {
        results.add(executeAsyncAccepted(query, values.get(finalI), context));
      } else {
        results.add(
            context
                .getBridge()
                .executeQueryAsync(query)
                .thenApply(rs -> toMutationResult(rs, values.get(finalI))));
      }
    }
    return convert(results);
  }

  private CompletionStage<List<Map<String, Object>>> executeAsPartOfBatch(
      List<Query> queries,
      Exception buildException,
      OperationDefinition operation,
      DataFetchingEnvironment environment,
      StargateGraphqlContext context) {
    int selections = environment.getOperationDefinition().getSelectionSet().getSelections().size();
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
              .getBridge()
              .executeBatchAsync(
                  QueryOuterClass.Batch.newBuilder()
                      .addAllQueries(batchContext.getQueries())
                      .setParameters(batchContext.getParameters())
                      .build()));
    }

    List<Map<String, Object>> values = environment.getArgument("values");

    if (containsDirective(operation, ASYNC_DIRECTIVE)) {
      // does not wait for the batch execution result, return the accepted response for all values
      // immediately
      return toListOfAcceptedMutationResults(values);
    } else {
      return batchContext.getExecutionFuture().thenApply(rows -> toBatchResults(rows, values));
    }
  }

  public static <T> CompletionStage<List<T>> convert(List<CompletionStage<T>> futures) {
    return futures.stream()
        .map(f -> f.thenApply(Stream::of))
        .reduce((a, b) -> a.thenCompose(xs -> b.thenApply(ys -> concat(xs, ys))))
        .map(f -> f.thenApply(s -> s.collect(Collectors.toList())))
        .orElse(CompletableFuture.completedFuture(Collections.emptyList()));
  }

  protected abstract List<Query> buildQueries(
      DataFetchingEnvironment environment, StargateGraphqlContext context);
}
