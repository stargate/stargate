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
import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.graphql.schema.schemafirst.processor.DeleteMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.FieldMappingModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class DeleteFetcher extends DynamicFetcher<Boolean> {

  private final DeleteMappingModel model;

  public DeleteFetcher(
      DeleteMappingModel model,
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
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {

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

    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .delete()
            .from(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .where(conditions)
            .build()
            .bind();

    authorizationService.authorizeDataWrite(
        authenticationSubject,
        entityModel.getKeyspaceName(),
        entityModel.getCqlName(),
        TypedKeyValue.forDML((BoundDelete) query),
        Scope.DELETE,
        SourceAPI.GRAPHQL);

    executeUnchecked(query, dataStore);

    return true;
  }
}
