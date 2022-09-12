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
import graphql.execution.DataFetcherResult;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.SourceLocation;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.api.common.cql.builder.Literal;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.cql.builder.ValueModifier;
import io.stargate.sgv2.graphql.schema.Uuids;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.InsertModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.EntityField;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
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

  public InsertFetcher(InsertModel model, MappingModel mappingModel, CqlKeyspaceDescribe keyspace) {
    super(model, mappingModel, keyspace);
  }

  @Override
  protected MutationPayload<DataFetcherResult<Object>> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityModel entityModel = model.getEntity();
    boolean isLwt = model.ifNotExists();
    String entityPrefixInResponse =
        model
            .getResponsePayload()
            .flatMap(ResponsePayloadModel::getEntityField)
            .map(EntityField::getName)
            .orElse(null);
    List<Map<String, Object>> inputs;
    if (model.isList()) {
      inputs = environment.getArgument(model.getEntityArgumentName());
    } else {
      inputs = Collections.singletonList(environment.getArgument(model.getEntityArgumentName()));
    }
    List<Query> queries = new ArrayList<>();
    List<List<TypedKeyValue>> primaryKeys = new ArrayList<>();
    List<Map<String, Value>> cqlValuesList = Lists.newArrayListWithCapacity(inputs.size());

    for (Map<String, Object> input : inputs) {
      Map<String, Value> cqlValues = buildCqlValues(entityModel, keyspace, input);
      Collection<ValueModifier> modifiers =
          cqlValues.entrySet().stream()
              .map(e -> ValueModifier.set(e.getKey(), e.getValue()))
              .collect(Collectors.toList());

      Optional<Long> timestamp =
          TimestampParser.parse(model.getCqlTimestampArgumentName(), environment);

      Query query = buildInsertQuery(entityModel, modifiers, timestamp, isLwt);

      List<TypedKeyValue> primaryKey = computePrimaryKey(entityModel, modifiers);

      queries.add(query);
      primaryKeys.add(primaryKey);
      cqlValuesList.add(cqlValues);
    }

    Function<List<MutationResult>, DataFetcherResult<Object>> resultBuilder =
        getResultBuilder(selectionSet, entityPrefixInResponse, inputs, cqlValuesList, environment);

    return new MutationPayload<>(queries, primaryKeys, resultBuilder);
  }

  private Query buildInsertQuery(
      EntityModel entityModel,
      Collection<ValueModifier> modifiers,
      Optional<Long> timestamp,
      boolean isLwt) {
    return new QueryBuilder()
        .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
        .value(modifiers)
        .ifNotExists(isLwt)
        .ttl(model.getTtl().orElse(null))
        .timestamp(timestamp.orElse(null))
        .parameters(buildParameters())
        .build();
  }

  private List<TypedKeyValue> computePrimaryKey(
      EntityModel entity, Collection<ValueModifier> modifiers) {
    List<TypedKeyValue> result = new ArrayList<>();
    for (FieldModel field : entity.getPrimaryKey()) {
      modifiers.stream()
          .filter(
              m ->
                  field.getCqlName().equals(m.target().columnName()) // targets this PK column
                      && m.target().mapKey() == null) // is column itself (not a map element)
          // Extract the value that was bound
          .map(m -> (Value) ((Literal) m.value()).get())
          .findFirst()
          .ifPresent(v -> result.add(new TypedKeyValue(field.getCqlName(), field.getCqlType(), v)));
    }
    return result;
  }

  private Function<List<MutationResult>, DataFetcherResult<Object>> getResultBuilder(
      DataFetchingFieldSelectionSet selectionSet,
      String entityPrefixInResponse,
      List<Map<String, Object>> inputs,
      List<Map<String, Value>> cqlValuesList,
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
      Map<String, Value> cqlValues,
      DataFetchingFieldSelectionSet selectionSet) {
    Map<String, Object> response = new LinkedHashMap<>();

    if (entityPrefixInResponse != null) {
      Map<String, Object> entityInResponse = new LinkedHashMap<>();
      response.put(entityPrefixInResponse, entityInResponse);
      if (queryResult instanceof MutationResult.Applied) {
        copyInputDataToResponse(input, cqlValues, entityInResponse);
      } else if (queryResult instanceof MutationResult.NotApplied) {
        MutationResult.NotApplied notApplied = (MutationResult.NotApplied) queryResult;
        notApplied
            .getRow()
            .ifPresent(
                row ->
                    copyRowToEntity(
                        row, notApplied.getColumns(), entityInResponse, model.getEntity()));
      }
    }
    if (selectionSet.contains(TechnicalField.APPLIED.getGraphqlName())) {
      response.put(
          TechnicalField.APPLIED.getGraphqlName(), queryResult instanceof MutationResult.Applied);
    }
    return response;
  }

  private Map<String, Object> buildEntityResponse(
      MutationResult queryResult, Map<String, Object> input, Map<String, Value> cqlValues) {

    Map<String, Object> entityResponse = new LinkedHashMap<>();
    if (queryResult instanceof MutationResult.Applied) {
      copyInputDataToResponse(input, cqlValues, entityResponse);
    } else if (queryResult instanceof MutationResult.NotApplied) {
      MutationResult.NotApplied notApplied = (MutationResult.NotApplied) queryResult;
      notApplied
          .getRow()
          .ifPresent(
              row ->
                  copyRowToEntity(row, notApplied.getColumns(), entityResponse, model.getEntity()));
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

  private Map<String, Value> buildCqlValues(
      EntityModel entityModel, CqlKeyspaceDescribe keyspace, Map<String, Object> input) {

    Map<String, Value> values = new HashMap<>();
    for (FieldModel column : entityModel.getAllColumns()) {
      String graphqlName = column.getGraphqlName();
      Value cqlValue;
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

  private Value generateUuid(TypeSpec cqlType) {
    if (cqlType.getBasic() == TypeSpec.Basic.UUID) {
      return Values.of(UUID.randomUUID());
    }
    if (cqlType.getBasic() == TypeSpec.Basic.TIMEUUID) {
      return Values.of(Uuids.timeBased());
    }
    throw new AssertionError("This shouldn't get called for CQL type " + cqlType);
  }

  /**
   * Copy all input arguments to the response (they might get overridden later if the query turned
   * out to be a failed LWT).
   */
  private void copyInputDataToResponse(
      Map<String, Object> input, Map<String, Value> cqlValues, Map<String, Object> entityData) {

    for (FieldModel column : model.getEntity().getAllColumns()) {
      String graphqlName = column.getGraphqlName();
      Object graphqlValue;
      if (input.containsKey(graphqlName)) {
        graphqlValue = input.get(graphqlName);
      } else if (column.isPrimaryKey()) {
        // The value is a generated UUID
        Value cqlValue = cqlValues.get(column.getCqlName());
        graphqlValue = Values.uuid(cqlValue).toString();
      } else {
        continue;
      }
      entityData.put(graphqlName, graphqlValue);
    }
  }
}
