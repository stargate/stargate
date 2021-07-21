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

import graphql.GraphQLException;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class MutationFetcher extends DmlFetcher<CompletableFuture<Map<String, Object>>> {
  protected MutationFetcher(Table table, NameMapping nameMapping) {
    super(table, nameMapping);
  }

  @Override
  protected CompletableFuture<Map<String, Object>> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    BoundQuery query = null;
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildStatement() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      query = buildQuery(environment, context);
    } catch (Exception e) {
      buildException = e;
    }
    OperationDefinition operation = environment.getOperationDefinition();

    if (containsDirective(operation, ATOMIC_DIRECTIVE)
        && operation.getSelectionSet().getSelections().size() > 1) {
      // There are more than one mutation in @atomic operation
      return executeAsPartOfBatch(environment, query, buildException, operation);
    }

    if (buildException != null) {
      CompletableFuture<Map<String, Object>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return f;
    }

    if (containsDirective(operation, ASYNC_DIRECTIVE)) {
      return executeAsyncAccepted(
          query, environment.getArgument("value"), __ -> buildParameters(environment), context);
    }

    // Execute as a single statement
    return context
        .getDataStore()
        .execute(query, __ -> buildParameters(environment))
        .thenApply(rs -> toMutationResult(rs, environment.getArgument("value")));
  }

  private CompletableFuture<Map<String, Object>> executeAsPartOfBatch(
      DataFetchingEnvironment environment,
      BoundQuery query,
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
    } else if (batchContext.add(query) == selections) {
      // All the statements were added successfully and this is the last selection
      batchContext.setExecutionResult(
          context
              .getDataStore()
              .batch(batchContext.getQueries(), __ -> batchContext.getParameters()));
    }

    if (containsDirective(operation, ASYNC_DIRECTIVE)) {
      // does not wait for the batch execution result, return the accepted response for the value
      // immediately
      return CompletableFuture.completedFuture(
          toAcceptedMutationResultWithOriginalValue(environment.getArgument("value")));
    } else {
      return batchContext
          .getExecutionFuture()
          .thenApply(rows -> toBatchResult(rows, environment.getArgument("value")));
    }
  }

  protected abstract BoundQuery buildQuery(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception;
}
