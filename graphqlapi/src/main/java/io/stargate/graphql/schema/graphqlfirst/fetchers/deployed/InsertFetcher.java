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
import io.stargate.db.datastore.DataStore;
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
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.EntityField;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.graphql.schema.graphqlfirst.util.Uuids;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class InsertFetcher extends DeployedFetcher<Map<String, Object>> {

  private final InsertModel model;

  public InsertFetcher(InsertModel model, MappingModel mappingModel) {
    super(mappingModel);
    this.model = model;
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment, DataStore dataStore, StargateGraphqlContext context)
      throws UnauthorizedException {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityModel entityModel = model.getEntity();
    boolean isLwt = model.ifNotExists();
    boolean responseContainsEntity =
        !model.getResponsePayload().isPresent()
            || model.getResponsePayload().flatMap(ResponsePayloadModel::getEntityField).isPresent();
    String entityPrefixInReponse =
        model
            .getResponsePayload()
            .flatMap(ResponsePayloadModel::getEntityField)
            .map(EntityField::getName)
            .orElse(null);
    Keyspace keyspace = dataStore.schema().keyspace(entityModel.getKeyspaceName());
    Map<String, Object> input = environment.getArgument(model.getEntityArgumentName());
    Map<String, Object> response = new LinkedHashMap<>();
    Map<String, Object> cqlValues = buildCqlValues(entityModel, keyspace, input);
    Collection<ValueModifier> modifiers =
        cqlValues.entrySet().stream()
            .map(e -> ValueModifier.set(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .value(modifiers)
            .ifNotExists(isLwt)
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

    ResultSet resultSet = executeUnchecked(query, Optional.empty(), Optional.empty(), dataStore);

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

    boolean applied;
    if (isLwt) {
      Row row = resultSet.one();
      applied = row.getBoolean("[applied]");
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
    }
    if (selectionSet.contains(TechnicalField.APPLIED.getGraphqlName())) {
      response.put(TechnicalField.APPLIED.getGraphqlName(), applied);
    }
    return response;
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
