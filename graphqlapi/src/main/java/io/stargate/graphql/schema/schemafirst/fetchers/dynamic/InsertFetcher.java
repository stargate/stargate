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
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.FieldMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.InsertMappingModel;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class InsertFetcher extends DynamicFetcher<Map<String, Object>> {

  private final InsertMappingModel model;

  public InsertFetcher(
      InsertMappingModel model,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
    this.model = model;
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {

    EntityMappingModel entityModel = model.getEntity();
    Map<String, Object> input = environment.getArgument(model.getEntityArgumentName());
    Map<String, Object> response = new LinkedHashMap<>();
    Collection<ValueModifier> setters = new ArrayList<>();
    for (FieldMappingModel column : entityModel.getAllColumns()) {
      String graphqlName = column.getGraphqlName();
      Object graphqlValue, cqlValue;
      if (input.containsKey(graphqlName)) {
        graphqlValue = input.get(graphqlName);
        cqlValue = toCqlValue(graphqlValue, column);
      } else if (column.isPrimaryKey()) {
        if (TypeHelper.isGraphqlId(column.getGraphqlType())) {
          cqlValue = generateUuid(column.getCqlType());
          graphqlValue = cqlValue.toString();
        } else {
          throw new IllegalArgumentException("Missing value for field " + graphqlName);
        }
      } else {
        continue;
      }
      setters.add(ValueModifier.set(column.getCqlName(), cqlValue));
      if (environment.getSelectionSet().contains(graphqlName)) {
        response.put(graphqlName, graphqlValue);
      }
    }

    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .value(setters)
            .build()
            .bind();

    authorizationService.authorizeDataWrite(
        authenticationSubject,
        entityModel.getKeyspaceName(),
        entityModel.getCqlName(),
        TypedKeyValue.forDML((BoundDMLQuery) query),
        Scope.MODIFY,
        SourceAPI.GRAPHQL);

    executeUnchecked(query, dataStore);

    return response;
  }

  private Object generateUuid(Column.ColumnType cqlType) {
    Object cqlValue;
    if (cqlType == Column.Type.Uuid) {
      cqlValue = UUID.randomUUID();
    } else if (cqlType == Column.Type.Timeuuid) {
      cqlValue = Uuids.timeBased();
    } else {
      // TODO catch this earlier in FieldMappingModel (more broadly all bad mappings)
      throw new IllegalArgumentException("Invalid CQL type for ID: " + cqlType);
    }
    return cqlValue;
  }
}
