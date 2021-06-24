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
    boolean responseContainsEntity = isEntityReturnType() || isPayloadModelReturnType();

    String entityPrefixInReponse =
        model
            .getResponsePayload()
            .flatMap(ResponsePayloadModel::getEntityField)
            .map(EntityField::getName)
            .orElse(null);
    Keyspace keyspace = context.getDataStore().schema().keyspace(entityModel.getKeyspaceName());
    List<Map<String, Object>> inputs;
    List<Map<String, Object>> responses = new ArrayList<>();
    // needed if the return type is a List of Booleans
    List<Boolean> appliedList = new ArrayList<>();
    if (model.isList()) {
      inputs = environment.getArgument(model.getEntityArgumentName());
    } else {
      inputs = Collections.singletonList(environment.getArgument(model.getEntityArgumentName()));
    }

    for (Map<String, Object> input : inputs) {
      boolean applied;
      Map<String, Object> response = new LinkedHashMap<>();
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

      if (responseContainsEntity) {
        Map<String, Object> entityData;
        if (entityPrefixInReponse == null) {
          entityData = response;
        } else {
          entityData = new LinkedHashMap<>();
          response.put(entityPrefixInReponse, entityData);
        }
        copyInputDataToEntity(input, cqlValues, entityData, entityModel);
      }

      if (isLwt) {
        Row row = resultSet.one();
        applied = row.getBoolean("[applied]");
        appliedList.add(applied);
        if (!applied && responseContainsEntity) {
          @SuppressWarnings("unchecked")
          Map<String, Object> entityData =
              (entityPrefixInReponse == null)
                  ? response
                  : (Map<String, Object>) response.get(entityPrefixInReponse);
          assert entityData != null;
          copyRowToEntity(row, entityData, model.getEntity());
        }
      } else {
        applied = true;
        appliedList.add(true);
      }
      if (selectionSet.contains(TechnicalField.APPLIED.getGraphqlName())) {
        response.put(TechnicalField.APPLIED.getGraphqlName(), applied);
      }
      responses.add(response);
    }

    if (model.isList()) {
      return returnListOfResponses(responses, appliedList);
    } else if (model.getReturnType() == OperationModel.SimpleReturnType.BOOLEAN) {
      // there must be only one response, return last (and only) applied
      return appliedList.get(0);
    } else {
      // there is only one response
      return responses.get(0);
    }
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
