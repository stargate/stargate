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
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.graphql.schema.graphqlfirst.processor.InsertModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.graphql.schema.graphqlfirst.util.Uuids;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
    Keyspace keyspace = dataStore.schema().keyspace(entityModel.getKeyspaceName());
    Map<String, Object> input = environment.getArgument(model.getEntityArgumentName());
    Map<String, Object> response = new LinkedHashMap<>();
    Collection<ValueModifier> setters = new ArrayList<>();
    for (FieldModel column : entityModel.getAllColumns()) {
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
      writeEntityField(
          graphqlName, graphqlValue, selectionSet, response, model.getResponsePayload());
    }

    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .insertInto(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .value(setters)
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
    populateResponseWithResultSet(
        selectionSet, response, isLwt, resultSet, model.getResponsePayload(), model.getEntity());

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
}
