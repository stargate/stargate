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

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundDMLQuery;
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
import java.util.stream.Collectors;

public class InsertFetcher extends MutationFetcher<InsertModel, Object> {

  public InsertFetcher(InsertModel model, MappingModel mappingModel) {
    super(model, mappingModel);
  }

  @Override
  protected Object get(DataFetchingEnvironment environment, StargateGraphqlContext context)
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
    List<Object> responses = new ArrayList<>();
    if (model.isList()) {
      inputs = environment.getArgument(model.getEntityArgumentName());
    } else {
      inputs = Collections.singletonList(environment.getArgument(model.getEntityArgumentName()));
    }

    for (Map<String, Object> input : inputs) {
      Map<String, Object> cqlValues = buildCqlValues(entityModel, keyspace, input);
      Collection<ValueModifier> modifiers =
          cqlValues.entrySet().stream()
              .map(e -> ValueModifier.set(e.getKey(), e.getValue()))
              .collect(Collectors.toList());

      Optional<Long> timestamp =
          TimestampParser.parse(model.getCqlTimestampArgumentName(), environment);

      AbstractBound<?> query =
          context
              .getDataStore()
              .queryBuilder()
              .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
              .value(modifiers)
              .ifNotExists(isLwt)
              .ttl(model.getTtl().orElse(null))
              .timestamp(timestamp.orElse(null))
              .build()
              .bind();

      context
          .getAuthorizationService()
          .authorizeDataWrite(
              context.getSubject(),
              entityModel.getKeyspaceName(),
              entityModel.getCqlName(),
              TypedKeyValue.forDML((BoundDMLQuery) query),
              Scope.MODIFY,
              SourceAPI.GRAPHQL);

      ResultSet resultSet = executeUnchecked(query, buildParameters(environment), context);

      // handle response
      Object response;
      if (isBooleanReturnType()) {
        response = buildBooleanResponse(isLwt, resultSet);
      } else if (isEntityReturnType()) {
        response = buildEntityResponse(isLwt, resultSet, entityPrefixInResponse, input, cqlValues);
      } else if (isPayloadModelReturnType()) {
        response =
            buildPayloadResponse(
                isLwt, resultSet, entityPrefixInResponse, input, cqlValues, selectionSet);
      } else {
        throw new UnsupportedOperationException(
            "The Insert operation does not support return type: " + model.getReturnType());
      }
      responses.add(response);
    }
    if (model.isList()) {
      return responses;
    } else {
      // there is only one response - entity or applied
      return responses.get(0);
    }
  }

  private Map<String, Object> buildPayloadResponse(
      boolean isLwt,
      ResultSet resultSet,
      String entityPrefixInResponse,
      Map<String, Object> input,
      Map<String, Object> cqlValues,
      DataFetchingFieldSelectionSet selectionSet) {
    Map<String, Object> response = new LinkedHashMap<>();

    Map<String, Object> entityData;
    if (entityPrefixInResponse == null) {
      entityData = response;
    } else {
      entityData = new LinkedHashMap<>();
      response.put(entityPrefixInResponse, entityData);
    }
    copyInputDataToEntity(input, cqlValues, entityData, model.getEntity());

    boolean applied;
    if (isLwt) {
      Row row = resultSet.one();
      applied = row.getBoolean("[applied]");
      if (!applied) {
        @SuppressWarnings("unchecked")
        Map<String, Object> entityDataInner =
            (entityPrefixInResponse == null)
                ? response
                : (Map<String, Object>) response.get(entityPrefixInResponse);
        assert entityDataInner != null;
        copyRowToEntity(row, entityDataInner, model.getEntity());
      }
    } else {
      applied = true;
    }
    if (selectionSet.contains(TechnicalField.APPLIED.getGraphqlName())) {
      response.put(TechnicalField.APPLIED.getGraphqlName(), applied);
    }
    return response;
  }

  private Map<String, Object> buildEntityResponse(
      boolean isLwt,
      ResultSet resultSet,
      String entityPrefixInResponse,
      Map<String, Object> input,
      Map<String, Object> cqlValues) {
    Map<String, Object> response = new LinkedHashMap<>();

    Map<String, Object> entityData;
    if (entityPrefixInResponse == null) {
      entityData = response;
    } else {
      entityData = new LinkedHashMap<>();
      response.put(entityPrefixInResponse, entityData);
    }
    copyInputDataToEntity(input, cqlValues, entityData, model.getEntity());

    if (isLwt) {
      Row row = resultSet.one();
      if (!row.getBoolean("[applied]")) {
        @SuppressWarnings("unchecked")
        Map<String, Object> entityDataInner =
            (entityPrefixInResponse == null)
                ? response
                : (Map<String, Object>) response.get(entityPrefixInResponse);
        assert entityDataInner != null;
        copyRowToEntity(row, entityDataInner, model.getEntity());
      }
    }
    return response;
  }

  private Boolean buildBooleanResponse(boolean isLwt, ResultSet resultSet) {
    if (isLwt) {
      Row row = resultSet.one();
      return row.getBoolean("[applied]");
    } else {
      return true;
    }
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

  private List<?> returnListOfResponses(
      List<Map<String, Object>> responses, List<Boolean> appliedList) {
    // this is a bulk insert, return a list of responses
    if (model.getReturnType() instanceof OperationModel.SimpleListReturnType) {
      // return list of booleans
      return appliedList;
    } else {
      // return list of entityModels or CustomPayloads
      return responses;
    }
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
  private void copyInputDataToEntity(
      Map<String, Object> input,
      Map<String, Object> cqlValues,
      Map<String, Object> entityData,
      EntityModel entityModel) {

    for (FieldModel column : entityModel.getAllColumns()) {
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
