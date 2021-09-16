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
package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import graphql.ExceptionWhileDataFetching;
import graphql.GraphQLException;
import graphql.execution.DataFetcherResult;
import graphql.language.OperationDefinition;
import graphql.language.SourceLocation;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.SchemaConstants;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MutationModel;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.graphql.schema.graphqlfirst.util.CompletableFutures;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** An INSERT, UPDATE or DELETE mutation. */
public abstract class MutationFetcher<MutationModelT extends MutationModel, ResultT>
    extends DeployedFetcher<CompletionStage<ResultT>> {

  protected final MutationModelT model;

  protected MutationFetcher(MutationModelT model, MappingModel mappingModel) {
    super(mappingModel);
    this.model = model;
  }

  protected abstract MutationPayload<ResultT> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws UnauthorizedException;

  @Override
  protected CompletableFuture<ResultT> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {

    MutationPayload<ResultT> payload = null;
    Parameters parameters = null;
    Exception buildException = null;
    try {
      payload = getPayload(environment, context);
      parameters = buildParameters();
    } catch (Exception e) {
      buildException = e;
    }

    if (isAtomic(environment, payload)) {
      return executeAsPartOfBatch(payload, parameters, buildException, environment, context);
    } else {
      return executeAsIndividualQueries(payload, parameters, buildException, context);
    }
  }

  /**
   * Processes the current operation if it is part of a larger atomic mutation, that requires
   * executing all the queries in a single CQL batch. For example:
   *
   * <pre>
   * mutation @atomic {
   *   operation1(args) { ... }
   *   operation2(args) { ... }
   * }
   * </pre>
   *
   * Our fetchers only cover a single operation, so we need to accumulate queries before we can
   * execute the batch.
   */
  private CompletableFuture<ResultT> executeAsPartOfBatch(
      MutationPayload<ResultT> payload,
      Parameters parameters,
      Exception buildException,
      DataFetchingEnvironment environment,
      StargateGraphqlContext context) {
    StargateGraphqlContext.BatchContext batchContext = context.getBatchContext();

    if (buildException == null && !batchContext.setParameters(parameters)) {
      buildException =
          new GraphQLException(
              "all the selections in an @atomic mutation must use the same consistency levels");
    }

    if (buildException != null) {
      batchContext.setExecutionResult(
          new GraphQLException(
              "@atomic mutation aborted because one of the operations failed "
                  + "(see other errors for details)"));
      return CompletableFutures.failedFuture(buildException);
    } else {
      int currentSelections = batchContext.add(payload.getQueries());
      // Check if we've processed all operations, e.g. with the example above total=2 and operation1
      // and operation2's fetchers each add 1.
      int totalSelections =
          environment.getOperationDefinition().getSelectionSet().getSelections().size();
      if (currentSelections == totalSelections && !batchContext.getExecutionFuture().isDone()) {
        // All operations processed and there were no errors => we are ready to batch and execute
        batchContext.setExecutionResult(
            context
                .getDataStore()
                .batch(batchContext.getQueries(), __ -> batchContext.getParameters()));
      }
    }
    return batchContext
        .getExecutionFuture()
        .thenApply(rows -> payload.getResultBuilder().apply(buildBatchResults(rows, payload)));
  }

  /**
   * Transforms the rows of a batch response to produce a 1:1 query<=>result mapping (which is
   * easier to handle in subclasses). This does not occur naturally because:
   *
   * <p>1) if the batch was applied, it returns no rows (if there were no LWTs) or just one row.
   *
   * <p>2) if the batch was not applied, it returns one row per LWT, but: a) they are in PK order,
   * not the order of the original queries; b) if there were also non-LWT queries, they have no row;
   * c) the current operation is part of a larger batch, so additional unrelated rows can be
   * present.
   *
   * <p>To solve all this, we use the PK or each query to look up the corresponding row.
   */
  private List<MutationResult> buildBatchResults(List<Row> rows, MutationPayload<ResultT> payload) {
    List<BoundQuery> queries = payload.getQueries();
    List<List<TypedKeyValue>> primaryKeys = payload.getPrimaryKeys();
    List<MutationResult> results = Lists.newArrayListWithCapacity(primaryKeys.size());
    if (isAppliedBatch(rows)) {
      for (int i = 0; i < queries.size(); i++) {
        results.add(MutationResult.Applied.INSTANCE);
      }
    } else {
      int i = 0;
      for (BoundQuery query : queries) {
        Table table = ((BoundDMLQuery) query).table();
        List<TypedKeyValue> primaryKey = primaryKeys.get(i);
        MutationResult.NotApplied result =
            rows.stream()
                .filter(row -> matches(row, table, primaryKey))
                .findFirst()
                .map(MutationResult.NotApplied::new)
                .orElse(MutationResult.NotApplied.NO_ROW);
        results.add(result);
        i += 1;
      }
    }
    return results;
  }

  private boolean matches(Row row, Table table, List<TypedKeyValue> primaryKey) {
    for (TypedKeyValue kv : primaryKey) {
      if (row.columns().stream()
              .noneMatch(
                  c ->
                      table.keyspace().equals(c.keyspace())
                          && table.name().equals(c.table())
                          && kv.getName().equals(c.name())
                          && kv.getType().equals(c.type()))
          || !kv.getValue().equals(row.getObject(kv.getName()))) {
        return false;
      }
    }
    return true;
  }

  private CompletableFuture<ResultT> executeAsIndividualQueries(
      MutationPayload<ResultT> payload,
      Parameters parameters,
      Exception buildException,
      StargateGraphqlContext context) {
    if (buildException != null) {
      return CompletableFutures.failedFuture(buildException);
    } else {
      List<CompletableFuture<MutationResult>> results =
          payload.getQueries().stream()
              .map(
                  query ->
                      context
                          .getDataStore()
                          .execute(query, __ -> parameters)
                          .thenApply(MutationResult::forSingleQuery)
                          .exceptionally(MutationResult.Failure::new))
              .collect(Collectors.toList());
      return CompletableFutures.sequence(results).thenApply(payload.getResultBuilder());
    }
  }

  private boolean isAtomic(DataFetchingEnvironment environment, MutationPayload<ResultT> payload) {
    OperationDefinition operation = environment.getOperationDefinition();
    return operation.getDirectives().stream()
            .anyMatch(d -> d.getName().equals(SchemaConstants.ATOMIC_DIRECTIVE))
        && (operation.getSelectionSet().getSelections().size() > 1
            || payload.getQueries().size() > 1);
  }

  protected Parameters buildParameters() {
    ConsistencyLevel consistencyLevel = model.getConsistencyLevel().orElse(DEFAULT_CONSISTENCY);
    ConsistencyLevel serialConsistencyLevel =
        model.getSerialConsistencyLevel().orElse(DEFAULT_SERIAL_CONSISTENCY);
    if (consistencyLevel == DEFAULT_CONSISTENCY
        && serialConsistencyLevel == DEFAULT_SERIAL_CONSISTENCY) {
      return DEFAULT_PARAMETERS;
    } else {
      return DEFAULT_PARAMETERS
          .toBuilder()
          .consistencyLevel(consistencyLevel)
          .serialConsistencyLevel(serialConsistencyLevel)
          .build();
    }
  }

  protected ExceptionWhileDataFetching toGraphqlError(
      MutationResult.Failure failure,
      SourceLocation location,
      DataFetchingEnvironment environment) {
    return toGraphqlError(failure.getError(), location, environment);
  }

  protected ExceptionWhileDataFetching toGraphqlError(
      Throwable error, SourceLocation location, DataFetchingEnvironment environment) {
    return new ExceptionWhileDataFetching(
        environment.getExecutionStepInfo().getPath(), error, location);
  }

  protected SourceLocation getCurrentFieldLocation(DataFetchingEnvironment environment) {
    return environment.getMergedField().getSingleField().getSourceLocation();
  }

  protected Function<List<MutationResult>, DataFetcherResult<Object>>
      getDeleteOrUpdateResultBuilder(DataFetchingEnvironment environment) {
    return queryResults -> {
      DataFetcherResult.Builder<Object> result = DataFetcherResult.newResult();
      assert queryResults.size() == 1;
      MutationResult queryResult = queryResults.get(0);
      if (queryResult instanceof MutationResult.Failure) {
        result.error(
            toGraphqlError(
                (MutationResult.Failure) queryResult,
                getCurrentFieldLocation(environment),
                environment));
      } else {
        boolean applied = queryResult instanceof MutationResult.Applied;
        OperationModel.ReturnType returnType = model.getReturnType();
        if (returnType == OperationModel.SimpleReturnType.BOOLEAN) {
          result.data(applied);
        } else {
          ResponsePayloadModel payload = (ResponsePayloadModel) returnType;
          Map<String, Object> data = new LinkedHashMap<>();
          if (payload.getTechnicalFields().contains(ResponsePayloadModel.TechnicalField.APPLIED)) {
            data.put(ResponsePayloadModel.TechnicalField.APPLIED.getGraphqlName(), applied);
          }
          if (payload.getEntityField().isPresent()) {
            Map<String, Object> entityData = new LinkedHashMap<>();
            if (queryResult instanceof MutationResult.NotApplied) {
              ((MutationResult.NotApplied) queryResult)
                  .getRow()
                  .ifPresent(row -> copyRowToEntity(row, entityData, model.getEntity()));
            }
            data.put(payload.getEntityField().get().getName(), entityData);
          }
          result.data(data);
        }
      }
      return result.build();
    };
  }
}
