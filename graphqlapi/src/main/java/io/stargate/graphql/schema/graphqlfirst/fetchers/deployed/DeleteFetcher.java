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

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.DeleteModel;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class DeleteFetcher extends DeployedFetcher<Object> {

  private final DeleteModel model;

  public DeleteFetcher(
      DeleteModel model,
      MappingModel mappingModel,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(mappingModel, authorizationService, dataStoreFactory);
    this.model = model;
  }

  @Override
  protected Object get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {

    EntityModel entityModel = model.getEntity();
    Keyspace keyspace = dataStore.schema().keyspace(entityModel.getKeyspaceName());

    // We're either getting the values from a single entity argument, or individual PK field
    // arguments:
    java.util.function.Predicate<String> hasArgument;
    Function<String, Object> getArgument;
    if (model.getEntityArgumentName().isPresent()) {
      Map<String, Object> entity = environment.getArgument(model.getEntityArgumentName().get());
      hasArgument = entity::containsKey;
      getArgument = entity::get;
    } else {
      hasArgument = environment::containsArgument;
      getArgument = environment::getArgument;
    }

    List<BuiltCondition> whereConditions =
        bind(model.getWhereConditions(), entityModel, hasArgument, getArgument, keyspace);
    AbstractBound<?> query =
        dataStore
            .queryBuilder()
            .delete()
            .from(entityModel.getKeyspaceName(), entityModel.getCqlName())
            .where(whereConditions)
            .ifExists(model.ifExists())
            .build()
            .bind();

    authorizationService.authorizeDataWrite(
        authenticationSubject,
        entityModel.getKeyspaceName(),
        entityModel.getCqlName(),
        TypedKeyValue.forDML((BoundDelete) query),
        Scope.DELETE,
        SourceAPI.GRAPHQL);

    ResultSet resultSet = executeUnchecked(query, Optional.empty(), Optional.empty(), dataStore);
    boolean applied = !model.ifExists() || resultSet.one().getBoolean("[applied]");

    ReturnType returnType = model.getReturnType();
    if (returnType == SimpleReturnType.BOOLEAN) {
      return applied;
    } else {
      ResponsePayloadModel payload = (ResponsePayloadModel) returnType;
      if (payload.getTechnicalFields().contains(TechnicalField.APPLIED)) {
        return ImmutableMap.of(TechnicalField.APPLIED.getGraphqlName(), applied);
      } else {
        return Collections.emptyMap();
      }
    }
  }
}
