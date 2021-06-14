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
import io.stargate.db.query.Modification;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.Value;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.*;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.EntityField;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class UpdateFetcher extends MutationFetcher<UpdateModel, Object> {

  public UpdateFetcher(UpdateModel model, MappingModel mappingModel) {
    super(model, mappingModel);
  }

  @Override
  protected Object get(
      DataFetchingEnvironment environment, DataStore dataStore, StargateGraphqlContext context)
      throws UnauthorizedException {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityModel entityModel = model.getEntity();
    Keyspace keyspace = dataStore.schema().keyspace(entityModel.getKeyspaceName());

    // We're either getting the values from a single entity argument, or individual PK field
    // arguments:
    Predicate<String> hasArgument;
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
            model.getWhereConditions(),
            hasArgument,
            getArgument,
            entityModel::validateForUpdate,
            keyspace);
    List<BuiltCondition> ifConditions =
        bindIf(model.getIfConditions(), hasArgument, getArgument, keyspace);
    boolean isLwt = !ifConditions.isEmpty() || model.ifExists();

    Collection<ValueModifier> modifiers;
    if (model.getIncrementModel().isPresent()) {
      modifiers =
          buildIncrementModifiers(
              model.getIncrementModel().get(), keyspace, hasArgument, getArgument);
    } else {
      modifiers = buildModifiers(entityModel, keyspace, hasArgument, getArgument);
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

    ResultSet resultSet = executeUnchecked(query, dataStore);

    boolean responseContainsEntity =
        model.getResponsePayload().flatMap(ResponsePayloadModel::getEntityField).isPresent();
    boolean applied;
    Map<String, Object> entityData = responseContainsEntity ? new LinkedHashMap<>() : null;
    if (isLwt) {
      Row row = resultSet.one();
      applied = row.getBoolean("[applied]");
      if (!applied && responseContainsEntity) {
        copyRowToEntity(row, entityData, model.getEntity());
      }
    } else {
      applied = true;
    }

    if (model.getReturnType() == SimpleReturnType.BOOLEAN) {
      return applied;
    } else {
      Map<String, Object> response = new LinkedHashMap<>();
      if (selectionSet.contains(ResponsePayloadModel.TechnicalField.APPLIED.getGraphqlName())) {
        response.put(ResponsePayloadModel.TechnicalField.APPLIED.getGraphqlName(), applied);
      }
      if (responseContainsEntity) {
        String prefix =
            model
                .getResponsePayload()
                .flatMap(ResponsePayloadModel::getEntityField)
                .map(EntityField::getName)
                .orElseThrow(AssertionError::new);
        response.put(prefix, entityData);
      }
      return response;
    }
  }

  private Collection<ValueModifier> buildIncrementModifiers(
      IncrementModel incrementModel,
      Keyspace keyspace,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument) {
    List<ValueModifier> modifiers = new ArrayList<>();
    FieldModel column = incrementModel.getField();

    if (hasArgument.test(incrementModel.getArgumentName())) {
      Object graphqlValue = getArgument.apply(incrementModel.getArgumentName());
      Modification.Operation operation =
          incrementModel.isPrepend()
              ? Modification.Operation.PREPEND
              : Modification.Operation.APPEND; // handles both increment and append
      modifiers.add(
          ValueModifier.of(
              column.getCqlName(),
              Value.of(toCqlValue(graphqlValue, column.getCqlType(), keyspace)),
              operation));
    }
    if (modifiers.isEmpty()) {
      throw new IllegalArgumentException(
          "Input object must have at least one non-PK field set for an update");
    }
    return modifiers;
  }

  private Collection<ValueModifier> buildModifiers(
      EntityModel entityModel,
      Keyspace keyspace,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument) {

    List<ValueModifier> modifiers = new ArrayList<>();
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
    return modifiers;
  }
}
