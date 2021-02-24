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
package io.stargate.graphql.schema.schemafirst.fetchers.dynamic;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.FieldMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.InsertMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.MappingModel;
import io.stargate.graphql.schema.schemafirst.processor.ResponseMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.ResponseMappingModel.TechnicalField;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class InsertFetcher extends DynamicFetcher<Map<String, Object>> {

  private final InsertMappingModel model;

  public InsertFetcher(
      InsertMappingModel model,
      MappingModel mappingModel,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(mappingModel, authenticationService, authorizationService, dataStoreFactory);
    this.model = model;
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityMappingModel entityModel = model.getEntity();
    boolean isLwt = model.ifNotExists();
    Keyspace keyspace = dataStore.schema().keyspace(entityModel.getKeyspaceName());
    Map<String, Object> input = environment.getArgument(model.getEntityArgumentName());
    Map<String, Object> response = new LinkedHashMap<>();
    Collection<ValueModifier> setters = new ArrayList<>();
    for (FieldMappingModel column : entityModel.getAllColumns()) {
      String graphqlName = column.getGraphqlName();
      Object graphqlValue;
      Object cqlValue;
      if (input.containsKey(graphqlName)) {
        graphqlValue = input.get(graphqlName);
        cqlValue = toCqlValue(graphqlValue, column.getCqlType(), keyspace);
      } else if (column.isPrimaryKey()) {
        if (TypeHelper.mapsToUuid(column.getGraphqlType())) {
          cqlValue = generateUuid(column.getCqlType());
          graphqlValue = cqlValue.toString();
        } else {
          throw new IllegalArgumentException("Missing value for field " + graphqlName);
        }
      } else {
        continue;
      }
      setters.add(ValueModifier.set(column.getCqlName(), cqlValue));
      // Echo the values back to the response now. We might override that later if the query turned
      // out to be a failed LWT.
      writeEntityField(graphqlName, graphqlValue, selectionSet, response);
    }

    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .value(setters)
            .ifNotExists(isLwt)
            .build()
            .bind();

    authorizationService.authorizeDataWrite(
        authenticationSubject,
        entityModel.getKeyspaceName(),
        entityModel.getCqlName(),
        TypedKeyValue.forDML((BoundDMLQuery) query),
        Scope.MODIFY,
        SourceAPI.GRAPHQL);

    ResultSet resultSet = executeUnchecked(query, dataStore);

    boolean applied;
    if (isLwt) {
      Row row = resultSet.one();
      applied = row.getBoolean("[applied]");
      if (!applied) {
        // The row contains the existing data, write it in the response.
        for (FieldMappingModel field : model.getEntity().getAllColumns()) {
          if (row.columns().stream().noneMatch(c -> c.name().equals(field.getCqlName()))) {
            continue;
          }
          Object cqlValue = row.getObject(field.getCqlName());
          writeEntityField(
              field.getGraphqlName(),
              toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()),
              selectionSet,
              response);
        }
      }
    } else {
      applied = true;
    }
    if (selectionSet.contains(TechnicalField.APPLIED.getGraphqlName())) {
      response.put(TechnicalField.APPLIED.getGraphqlName(), applied);
    }
    return response;
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

  /** Writes an entity field in the response. */
  private void writeEntityField(
      String fieldName,
      Object value,
      DataFetchingFieldSelectionSet selectionSet,
      Map<String, Object> responseMap) {

    // Determine if we need an additional level of nesting. This happens if the mutation returns a
    // payload type that contains the entity.
    String rootPath = null;
    if (model.getResponsePayloadType().isPresent()) {
      ResponseMappingModel payloadType = model.getResponsePayloadType().get();
      if (!payloadType.getEntityField().isPresent()) {
        // This can happen if the payload only contains "technical" fields, like `applied`. In that
        // case we never need to write any field.
        return;
      }
      rootPath = payloadType.getEntityField().get().getName();
    }

    // Check if the GraphQL query asked for that field.
    String selectionPattern = (rootPath == null) ? fieldName : rootPath + '/' + fieldName;
    if (!selectionSet.contains(selectionPattern)) {
      return;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> targetMap =
        (rootPath == null)
            ? responseMap
            : (Map<String, Object>)
                responseMap.computeIfAbsent(rootPath, __ -> new HashMap<String, Object>());
    targetMap.put(fieldName, value);
  }
}
