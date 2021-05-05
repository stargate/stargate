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
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.UpdateModel;
import java.util.*;

public class UpdateFetcher extends DeployedFetcher<Boolean> {

  private final UpdateModel model;

  public UpdateFetcher(
      UpdateModel model,
      MappingModel mappingModel,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(mappingModel, authorizationService, dataStoreFactory);
    this.model = model;
  }

  @Override
  protected Boolean get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {

    EntityModel entityModel = model.getEntity();
    Keyspace keyspace = dataStore.schema().keyspace(entityModel.getKeyspaceName());
    Map<String, Object> input = environment.getArgument(model.getEntityArgumentName());

    List<BuiltCondition> whereConditions =
        bind(
            entityModel.getPrimaryKeyWhereConditions(),
            entityModel,
            input::containsKey,
            input::get,
            keyspace);

    Collection<ValueModifier> modifiers = new ArrayList<>();
    for (FieldModel column : entityModel.getRegularColumns()) {
      String graphqlName = column.getGraphqlName();
      if (input.containsKey(graphqlName)) {
        Object graphqlValue = input.get(graphqlName);
        modifiers.add(
            ValueModifier.set(
                column.getCqlName(), toCqlValue(graphqlValue, column.getCqlType(), keyspace)));
      }
    }
    if (modifiers.isEmpty()) {
      throw new IllegalArgumentException(
          "Input object must have at least one non-PK field set for an update");
    }

    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .update(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .value(modifiers)
            .where(whereConditions)
            .build()
            .bind();

    authorizationService.authorizeDataWrite(
        authenticationSubject,
        entityModel.getKeyspaceName(),
        entityModel.getCqlName(),
        TypedKeyValue.forDML((BoundDMLQuery) query),
        Scope.MODIFY,
        SourceAPI.GRAPHQL);

    executeUnchecked(query, Optional.empty(), Optional.empty(), dataStore);

    return true;
  }
}
