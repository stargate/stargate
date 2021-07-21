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

import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.Modification;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.Value;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.graphql.schema.graphqlfirst.processor.IncrementModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.processor.UpdateModel;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class UpdateFetcher extends MutationFetcher<UpdateModel, DataFetcherResult<Object>> {

  public UpdateFetcher(UpdateModel model, MappingModel mappingModel) {
    super(model, mappingModel);
  }

  @Override
  protected MutationPayload<DataFetcherResult<Object>> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws UnauthorizedException {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityModel entityModel = model.getEntity();
    Keyspace keyspace = context.getDataStore().schema().keyspace(entityModel.getKeyspaceName());

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

    Collection<ValueModifier> modifiers;
    if (!model.getIncrementModels().isEmpty()) {
      modifiers =
          buildIncrementModifiers(model.getIncrementModels(), keyspace, hasArgument, getArgument);
    } else {
      modifiers = buildModifiers(entityModel, keyspace, hasArgument, getArgument);
    }

    // we are supporting BigInt and GraphQLString for ISO date format
    Optional<Long> timestamp =
        TimestampParser.parse(model.getCqlTimestampArgumentName(), environment);
    AbstractBound<?> query =
        buildUpdateQuery(entityModel, modifiers, whereConditions, ifConditions, timestamp, context);

    List<TypedKeyValue> primaryKey = TypedKeyValue.forDML((BoundDMLQuery) query);
    authorizeUpdate(entityModel, primaryKey, context);

    Function<List<MutationResult>, DataFetcherResult<Object>> resultBuilder =
        getDeleteOrUpdateResultBuilder(environment);

    return new MutationPayload<>(query, primaryKey, resultBuilder);
  }

  private Collection<ValueModifier> buildIncrementModifiers(
      List<IncrementModel> incrementModels,
      Keyspace keyspace,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument) {
    List<ValueModifier> modifiers = new ArrayList<>();

    for (IncrementModel incrementModel : incrementModels) {
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

  private AbstractBound<?> buildUpdateQuery(
      EntityModel entityModel,
      Collection<ValueModifier> modifiers,
      List<BuiltCondition> whereConditions,
      List<BuiltCondition> ifConditions,
      Optional<Long> timestamp,
      StargateGraphqlContext context) {
    return context
        .getDataStore()
        .queryBuilder()
        .update(entityModel.getKeyspaceName(), entityModel.getCqlName())
        .ttl(model.getTtl().orElse(null))
        .timestamp(timestamp.orElse(null))
        .value(modifiers)
        .where(whereConditions)
        .ifs(ifConditions)
        .ifExists(model.ifExists())
        .build()
        .bind();
  }

  private void authorizeUpdate(
      EntityModel entityModel, List<TypedKeyValue> primaryKey, StargateGraphqlContext context)
      throws UnauthorizedException {
    context
        .getAuthorizationService()
        .authorizeDataWrite(
            context.getSubject(),
            entityModel.getKeyspaceName(),
            entityModel.getCqlName(),
            primaryKey,
            Scope.MODIFY,
            SourceAPI.GRAPHQL);
  }
}
