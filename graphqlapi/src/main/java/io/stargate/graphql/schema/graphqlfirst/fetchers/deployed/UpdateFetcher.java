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
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.*;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.*;
import java.util.function.Function;

public class UpdateFetcher extends DeployedFetcher<Object> {

  private final UpdateModel model;

  public UpdateFetcher(UpdateModel model, MappingModel mappingModel) {
    super(mappingModel);
    this.model = model;
  }

  @Override
  protected Object get(
      DataFetchingEnvironment environment, DataStore dataStore, StargateGraphqlContext context)
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
        bindWhere(
            entityModel.getPrimaryKeyWhereConditions(),
            entityModel,
            hasArgument,
            getArgument,
            keyspace);
    List<BuiltCondition> ifConditions =
        bindIf(model.getIfConditions(), hasArgument, getArgument, keyspace);

    Collection<ValueModifier> modifiers = new ArrayList<>();
    for (FieldModel column : entityModel.getRegularColumns()) {
      String graphqlName = column.getGraphqlName();
      if (hasArgument.test(graphqlName)) {
        Object graphqlValue = getArgument.apply(graphqlName);
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
            .ifs(ifConditions)
            .ifExists(model.ifExists())
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

    boolean applied;
    if (!ifConditions.isEmpty() || model.ifExists()) {
      applied = resultSet.one().getBoolean("[applied]");
    } else {
      applied = !model.ifExists();
    }

    OperationModel.ReturnType returnType = model.getReturnType();
    if (returnType == OperationModel.SimpleReturnType.BOOLEAN) {
      return applied;
    } else {
      ResponsePayloadModel payload = (ResponsePayloadModel) returnType;
      if (payload.getTechnicalFields().contains(ResponsePayloadModel.TechnicalField.APPLIED)) {
        return ImmutableMap.of(
            ResponsePayloadModel.TechnicalField.APPLIED.getGraphqlName(), applied);
      } else {
        return Collections.emptyMap();
      }
    }
  }
}
