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

import graphql.GraphQLException;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public abstract class MutationFetcher extends DmlFetcher<CompletionStage<Map<String, Object>>> {

  protected MutationFetcher(String keyspaceName, CqlTable table, NameMapping nameMapping) {
    super(keyspaceName, table, nameMapping);
  }

  @Override
  protected CompletionStage<Map<String, Object>> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception {

    Query query = null;
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildQuery() could throw an unchecked exception.
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
      return executeAsPartOfBatch(query, buildException, operation, environment, context);
    }

    if (buildException != null) {
      return Futures.failedFuture(buildException);
    }

    QueryParameters parameters = buildParameters(environment);
    query = Query.newBuilder(query).setParameters(parameters).build();

    if (containsDirective(operation, ASYNC_DIRECTIVE)) {
      return executeAsyncAccepted(query, environment.getArgument("value"), context);
    }

    // Execute as a single statement
    return context
        .getBridge()
        .executeQueryAsync(query)
        .thenApply(response -> toMutationResult(response, environment.getArgument("value")));
  }

  private CompletionStage<Map<String, Object>> executeAsPartOfBatch(
      Query query,
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
    } else if (batchContext.add(query) == selections) {
      // All the statements were added successfully and this is the last selection
      batchContext.setExecutionResult(
          context
              .getBridge()
              .executeBatchAsync(
                  Batch.newBuilder()
                      .addAllQueries(batchContext.getQueries())
                      .setParameters(batchContext.getParameters())
                      .build()));
    }

    if (containsDirective(operation, ASYNC_DIRECTIVE)) {
      // does not wait for the batch execution result, return the accepted response for the value
      // immediately
      return CompletableFuture.completedFuture(
          toAcceptedMutationResult(environment.getArgument("value")));
    } else {
      return batchContext
          .getExecutionFuture()
          .thenApply(resultSet -> toBatchResult(resultSet, environment.getArgument("value")));
    }
  }

  protected abstract Query buildQuery(
      DataFetchingEnvironment environment, StargateGraphqlContext context);
}
