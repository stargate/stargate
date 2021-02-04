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
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.FieldMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.UpdateMappingModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class UpdateFetcher extends DynamicFetcher<Boolean> {

  private final UpdateMappingModel model;

  public UpdateFetcher(
      UpdateMappingModel model,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
    this.model = model;
  }

  @Override
  protected Boolean get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject) {

    EntityMappingModel entityModel = model.getEntity();
    Map<String, Object> input = environment.getArgument(model.getEntityArgumentName());

    Collection<BuiltCondition> conditions = new ArrayList<>();
    for (FieldMappingModel column : entityModel.getPrimaryKey()) {
      String graphqlName = column.getGraphqlName();
      Object graphqlValue = input.get(graphqlName);
      if (graphqlValue == null) {
        throw new IllegalArgumentException("Missing value for field " + graphqlName);
      } else {
        conditions.add(
            BuiltCondition.of(column.getCqlName(), Predicate.EQ, toCqlValue(graphqlValue, column)));
      }
    }

    Collection<ValueModifier> modifiers = new ArrayList<>();
    for (FieldMappingModel column : entityModel.getRegularColumns()) {
      String graphqlName = column.getGraphqlName();
      if (input.containsKey(graphqlName)) {
        Object graphqlValue = input.get(graphqlName);
        modifiers.add(ValueModifier.set(column.getCqlName(), toCqlValue(graphqlValue, column)));
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
            .where(conditions)
            .build()
            .bind();
    executeUnchecked(query, dataStore);

    return true;
  }
}
