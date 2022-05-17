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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import com.google.common.collect.Lists;
import graphql.ExceptionWhileDataFetching;
import graphql.GraphQLException;
import graphql.execution.DataFetcherResult;
import graphql.language.OperationDefinition;
import graphql.language.SourceLocation;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Literal;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.proto.Rows;
import io.stargate.sgv2.graphql.schema.SchemaConstants;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MutationModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

/** An INSERT, UPDATE or DELETE mutation. */
public abstract class MutationFetcher<MutationModelT extends MutationModel, ResultT>
    extends DeployedFetcher<CompletionStage<ResultT>> {

  protected final MutationModelT model;

  protected MutationFetcher(
      MutationModelT model, MappingModel mappingModel, CqlKeyspaceDescribe keyspace) {
    super(mappingModel, keyspace);
    this.model = model;
  }

  protected abstract MutationPayload<ResultT> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context);

  @Override
  protected CompletionStage<ResultT> get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {

    MutationPayload<ResultT> payload = null;
    Exception buildException = null;
    try {
      payload = getPayload(environment, context);
    } catch (Exception e) {
      buildException = e;
    }

    if (isAtomic(environment, payload)) {
      return executeAsPartOfBatch(payload, buildException, environment, context);
    } else {
      return executeAsIndividualQueries(payload, buildException, context);
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
      Exception buildException,
      DataFetchingEnvironment environment,
      StargateGraphqlContext context) {
    StargateGraphqlContext.BatchContext batchContext = context.getBatchContext();

    for (Query query : payload.getQueries()) {
      if (query.hasParameters() && !batchContext.setParameters(query.getParameters())) {
        buildException =
            new GraphQLException(
                "all the selections in an @atomic mutation must use the same consistency levels");
        break;
      }
    }
    if (buildException != null) {
      batchContext.setExecutionResult(
          new GraphQLException(
              "@atomic mutation aborted because one of the operations failed "
                  + "(see other errors for details)"));
      return Futures.failedFuture(buildException);
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
                .getBridge()
                .executeBatchAsync(
                    QueryOuterClass.Batch.newBuilder()
                        .addAllQueries(batchContext.getQueries())
                        .setParameters(batchContext.getParameters())
                        .build()));
      }
    }
    return batchContext
        .getExecutionFuture()
        .thenApply(
            response ->
                payload
                    .getResultBuilder()
                    .apply(buildBatchResults(response.getResultSet(), payload)));
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
  private List<MutationResult> buildBatchResults(
      ResultSet resultSet, MutationPayload<ResultT> payload) {
    List<List<TypedKeyValue>> primaryKeys = payload.getPrimaryKeys();
    List<MutationResult> results = Lists.newArrayListWithCapacity(primaryKeys.size());
    if (isApplied(resultSet)) {
      for (int i = 0; i < primaryKeys.size(); i++) {
        results.add(MutationResult.Applied.INSTANCE);
      }
    } else {
      for (List<TypedKeyValue> primaryKey : primaryKeys) {
        Optional<Row> row =
            resultSet.getRowsList().stream()
                .filter(r -> matches(r, resultSet.getColumnsList(), primaryKey))
                .findFirst();
        MutationResult.NotApplied result =
            new MutationResult.NotApplied(row, resultSet.getColumnsList());
        results.add(result);
      }
    }
    return results;
  }

  private boolean matches(Row row, List<ColumnSpec> columns, List<TypedKeyValue> primaryKey) {
    for (TypedKeyValue kv : primaryKey) {
      if (columns.stream()
              .noneMatch(c -> kv.getName().equals(c.getName()) && kv.getType().equals(c.getType()))
          || !kv.getValue().equals(Rows.getValue(row, kv.getName(), columns))) {
        return false;
      }
    }
    return true;
  }

  private CompletionStage<ResultT> executeAsIndividualQueries(
      MutationPayload<ResultT> payload, Exception buildException, StargateGraphqlContext context) {
    if (buildException != null) {
      return Futures.failedFuture(buildException);
    } else {
      List<CompletionStage<MutationResult>> results =
          payload.getQueries().stream()
              .map(
                  query ->
                      context
                          .getBridge()
                          .executeQueryAsync(query)
                          .thenApply(MutationResult::forSingleQuery)
                          .exceptionally(MutationResult.Failure::new))
              .collect(Collectors.toList());
      return Futures.sequence(results).thenApply(payload.getResultBuilder());
    }
  }

  private boolean isAtomic(DataFetchingEnvironment environment, MutationPayload<ResultT> payload) {
    OperationDefinition operation = environment.getOperationDefinition();
    return operation.getDirectives().stream()
            .anyMatch(d -> d.getName().equals(SchemaConstants.ATOMIC_DIRECTIVE))
        && (operation.getSelectionSet().getSelections().size() > 1
            || payload.getQueries().size() > 1);
  }

  protected QueryParameters buildParameters() {
    ConsistencyValue consistencyLevel =
        model
            .getConsistencyLevel()
            .map(c -> ConsistencyValue.newBuilder().setValue(c).build())
            .orElse(DEFAULT_CONSISTENCY);
    ConsistencyValue serialConsistencyLevel =
        model
            .getSerialConsistencyLevel()
            .map(c -> ConsistencyValue.newBuilder().setValue(c).build())
            .orElse(DEFAULT_SERIAL_CONSISTENCY);
    if (consistencyLevel == DEFAULT_CONSISTENCY
        && serialConsistencyLevel == DEFAULT_SERIAL_CONSISTENCY) {
      return DEFAULT_PARAMETERS;
    } else {
      return DEFAULT_PARAMETERS
          .toBuilder()
          .setConsistency(consistencyLevel)
          .setSerialConsistency(serialConsistencyLevel)
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
              MutationResult.NotApplied notApplied = (MutationResult.NotApplied) queryResult;
              notApplied
                  .getRow()
                  .ifPresent(
                      row ->
                          copyRowToEntity(
                              row, notApplied.getColumns(), entityData, model.getEntity()));
            }
            data.put(payload.getEntityField().get().getName(), entityData);
          }
          result.data(data);
        }
      }
      return result.build();
    };
  }

  /**
   * Given the where conditions generated for the current GraphQL query, try to identify the primary
   * key targeted by the generated CQL query.
   *
   * <p>If the query targets multiple rows, we'll get a partial PK, but it doesn't matter because we
   * won't end up using it (see explanations in {@link MutationPayload#getPrimaryKeys()}).
   */
  protected List<TypedKeyValue> computePrimaryKey(
      EntityModel entity, List<BuiltCondition> whereConditions) {
    List<TypedKeyValue> result = new ArrayList<>();
    for (FieldModel field : entity.getPrimaryKey()) {
      whereConditions.stream()
          .filter(
              c ->
                  field.getCqlName().equals(c.lhs().columnName()) // targets this PK column
                      && !c.lhs().value().isPresent() // is column itself (not a map element)
                      && c.predicate() == Predicate.EQ)
          // Extract the value that was bound
          .map(c -> (QueryOuterClass.Value) ((Literal) c.value()).get())
          .findFirst()
          .ifPresent(v -> result.add(new TypedKeyValue(field.getCqlName(), field.getCqlType(), v)));
    }
    return result;
  }
}
