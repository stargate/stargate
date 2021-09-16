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
import graphql.execution.DataFetcherResult;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.SourceLocation;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.graphql.schema.graphqlfirst.processor.InsertModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.EntityField;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.graphql.schema.graphqlfirst.util.Uuids;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InsertFetcher extends MutationFetcher<InsertModel, DataFetcherResult<Object>> {

  public InsertFetcher(InsertModel model, MappingModel mappingModel) {
    super(model, mappingModel);
  }

  @Override
  protected MutationPayload<DataFetcherResult<Object>> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws UnauthorizedException {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityModel entityModel = model.getEntity();
    boolean isLwt = model.ifNotExists();
    String entityPrefixInResponse =
        model
            .getResponsePayload()
            .flatMap(ResponsePayloadModel::getEntityField)
            .map(EntityField::getName)
            .orElse(null);
    Keyspace keyspace = context.getDataStore().schema().keyspace(entityModel.getKeyspaceName());
    List<Map<String, Object>> inputs;
    if (model.isList()) {
      inputs = environment.getArgument(model.getEntityArgumentName());
    } else {
      inputs = Collections.singletonList(environment.getArgument(model.getEntityArgumentName()));
    }
    List<BoundQuery> queries = new ArrayList<>();
    List<List<TypedKeyValue>> primaryKeys = new ArrayList<>();
    List<Map<String, Object>> cqlValuesList = Lists.newArrayListWithCapacity(inputs.size());

    for (Map<String, Object> input : inputs) {
      Map<String, Object> cqlValues = buildCqlValues(entityModel, keyspace, input);
      Collection<ValueModifier> modifiers =
          cqlValues.entrySet().stream()
              .map(e -> ValueModifier.set(e.getKey(), e.getValue()))
              .collect(Collectors.toList());

      Optional<Long> timestamp =
          TimestampParser.parse(model.getCqlTimestampArgumentName(), environment);

      AbstractBound<?> query = buildInsertQuery(entityModel, modifiers, timestamp, isLwt, context);

      List<TypedKeyValue> primaryKey = TypedKeyValue.forDML((BoundDMLQuery) query);
      authorizeInsert(entityModel, primaryKey, context);

      queries.add(query);
      primaryKeys.add(primaryKey);
      cqlValuesList.add(cqlValues);
    }

    Function<List<MutationResult>, DataFetcherResult<Object>> resultBuilder =
        getResultBuilder(selectionSet, entityPrefixInResponse, inputs, cqlValuesList, environment);

    return new MutationPayload<>(queries, primaryKeys, resultBuilder);
  }

  private AbstractBound<?> buildInsertQuery(
      EntityModel entityModel,
      Collection<ValueModifier> modifiers,
      Optional<Long> timestamp,
      boolean isLwt,
      StargateGraphqlContext context) {
    return context
        .getDataStore()
        .queryBuilder()
        .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
        .value(modifiers)
        .ifNotExists(isLwt)
        .ttl(model.getTtl().orElse(null))
        .timestamp(timestamp.orElse(null))
        .build()
        .bind();
  }

  private void authorizeInsert(
      EntityModel entityModel, List<TypedKeyValue> primaryKey, StargateGraphqlContext context)
      throws UnauthorizedException {
    context
        .getAuthorizationService()
        .authorizeDataWrite(
            context.getSubject(),
            entityModel.getKeyspaceName(),
            entityModel.getCqlName(),
            primaryKey,
            Scope.MODIFY,
            SourceAPI.GRAPHQL);
  }

  private Function<List<MutationResult>, DataFetcherResult<Object>> getResultBuilder(
      DataFetchingFieldSelectionSet selectionSet,
      String entityPrefixInResponse,
      List<Map<String, Object>> inputs,
      List<Map<String, Object>> cqlValuesList,
      DataFetchingEnvironment environment) {
    return queryResults -> {
      DataFetcherResult.Builder<Object> result = DataFetcherResult.newResult();
      List<Object> data = new ArrayList<>();
      int i = 0;
      for (MutationResult queryResult : queryResults) {
        if (queryResult instanceof MutationResult.Failure) {
          result.error(
              toGraphqlError(
                  (MutationResult.Failure) queryResult,
                  getInputLocation(i, environment),
                  environment));
          data.add(null);
        } else {
          if (isBooleanReturnType()) {
            data.add(queryResult instanceof MutationResult.Applied);
          } else if (isEntityReturnType()) {
            data.add(buildEntityResponse(queryResult, inputs.get(i), cqlValuesList.get(i)));
          } else if (isPayloadModelReturnType()) {
            data.add(
                buildPayloadResponse(
                    queryResult,
                    entityPrefixInResponse,
                    inputs.get(i),
                    cqlValuesList.get(i),
                    selectionSet));
          } else {
            // Should never happen since the model builder has checked already
            result.error(
                toGraphqlError(
                    new IllegalArgumentException(
                        "Unsupported return type: " + model.getReturnType()),
                    getCurrentFieldLocation(environment),
                    environment));
            data.add(null);
          }
        }
        i += 1;
      }
      return result.data(model.isList() ? data : data.get(0)).build();
    };
  }

  /**
   * Computes the location of the current data being inserted. This allows us to provide a better
   * location when reporting errors (especially for bulk inserts).
   */
  private SourceLocation getInputLocation(int inputIndex, DataFetchingEnvironment environment) {
    Argument argument =
        environment.getField().getArguments().stream()
            .filter(a -> a.getName().equals(model.getEntityArgumentName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Entity argument should be present"));
    if (model.isList()) {
      ArrayValue arrayValue = ((ArrayValue) argument.getValue());
      return arrayValue.getValues().get(inputIndex).getSourceLocation();
    } else {
      return argument.getSourceLocation();
    }
  }

  private Map<String, Object> buildPayloadResponse(
      MutationResult queryResult,
      String entityPrefixInResponse,
      Map<String, Object> input,
      Map<String, Object> cqlValues,
      DataFetchingFieldSelectionSet selectionSet) {
    Map<String, Object> response = new LinkedHashMap<>();

    if (entityPrefixInResponse != null) {
      Map<String, Object> entityInResponse = new LinkedHashMap<>();
      response.put(entityPrefixInResponse, entityInResponse);
      if (queryResult instanceof MutationResult.Applied) {
        copyInputDataToResponse(input, cqlValues, entityInResponse);
      } else if (queryResult instanceof MutationResult.NotApplied) {
        ((MutationResult.NotApplied) queryResult)
            .getRow()
            .ifPresent(row -> copyRowToEntity(row, entityInResponse, model.getEntity()));
      }
    }
    if (selectionSet.contains(TechnicalField.APPLIED.getGraphqlName())) {
      response.put(
          TechnicalField.APPLIED.getGraphqlName(), queryResult instanceof MutationResult.Applied);
    }
    return response;
  }

  private Map<String, Object> buildEntityResponse(
      MutationResult queryResult, Map<String, Object> input, Map<String, Object> cqlValues) {

    Map<String, Object> entityResponse = new LinkedHashMap<>();
    if (queryResult instanceof MutationResult.Applied) {
      copyInputDataToResponse(input, cqlValues, entityResponse);
    } else if (queryResult instanceof MutationResult.NotApplied) {
      ((MutationResult.NotApplied) queryResult)
          .getRow()
          .ifPresent(row -> copyRowToEntity(row, entityResponse, model.getEntity()));
    }
    return entityResponse;
  }

  // returns true if the return type is a boolean or [boolean]
  private boolean isBooleanReturnType() {
    OperationModel.ReturnType returnType = model.getReturnType();
    return returnType == OperationModel.SimpleReturnType.BOOLEAN
        || returnType instanceof OperationModel.SimpleListReturnType
            && ((OperationModel.SimpleListReturnType) returnType)
                .getSimpleReturnType()
                .equals(OperationModel.SimpleReturnType.BOOLEAN);
  }

  private boolean isPayloadModelReturnType() {
    return model.getResponsePayload().flatMap(ResponsePayloadModel::getEntityField).isPresent();
  }

  private boolean isEntityReturnType() {
    return model.getReturnType() instanceof OperationModel.EntityReturnType
        || model.getReturnType() instanceof OperationModel.EntityListReturnType;
  }

  private Map<String, Object> buildCqlValues(
      EntityModel entityModel, Keyspace keyspace, Map<String, Object> input) {

    Map<String, Object> values = new HashMap<>();
    for (FieldModel column : entityModel.getAllColumns()) {
      String graphqlName = column.getGraphqlName();
      Object cqlValue;
      if (input.containsKey(graphqlName)) {
        Object graphqlValue = input.get(graphqlName);
        cqlValue = toCqlValue(graphqlValue, column.getCqlType(), keyspace);
      } else if (column.isPrimaryKey()) {
        if (TypeHelper.mapsToUuid(column.getGraphqlType())) {
          cqlValue = generateUuid(column.getCqlType());
        } else {
          throw new IllegalArgumentException("Missing value for field " + graphqlName);
        }
      } else {
        continue;
      }
      values.put(column.getCqlName(), cqlValue);
    }
    return values;
  }

  private Object generateUuid(Column.ColumnType cqlType) {
    if (cqlType == Column.Type.Uuid) {
      return UUID.randomUUID();
    }
    if (cqlType == Column.Type.Timeuuid) {
      return Uuids.timeBased();
    }
    throw new AssertionError("This shouldn't get called for CQL type " + cqlType);
  }

  /**
   * Copy all input arguments to the response (they might get overridden later if the query turned
   * out to be a failed LWT).
   */
  private void copyInputDataToResponse(
      Map<String, Object> input, Map<String, Object> cqlValues, Map<String, Object> entityData) {

    for (FieldModel column : model.getEntity().getAllColumns()) {
      String graphqlName = column.getGraphqlName();
      Object graphqlValue;
      if (input.containsKey(graphqlName)) {
        graphqlValue = input.get(graphqlName);
      } else if (column.isPrimaryKey()) {
        // The value is a generated UUID
        Object cqlValue = cqlValues.get(column.getCqlName());
        assert cqlValue instanceof UUID;
        graphqlValue = cqlValue.toString();
      } else {
        continue;
      }
      entityData.put(graphqlName, graphqlValue);
    }
  }
}
