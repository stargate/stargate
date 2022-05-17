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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Term;
import io.stargate.sgv2.common.cql.builder.ValueModifier;
import io.stargate.sgv2.common.cql.builder.ValueModifier.Operation;
import io.stargate.sgv2.common.cql.builder.ValueModifier.Target;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.IncrementModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.UpdateModel;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class UpdateFetcher extends MutationFetcher<UpdateModel, DataFetcherResult<Object>> {

  public UpdateFetcher(UpdateModel model, MappingModel mappingModel, CqlKeyspaceDescribe keyspace) {
    super(model, mappingModel, keyspace);
  }

  @Override
  protected MutationPayload<DataFetcherResult<Object>> getPayload(
      DataFetchingEnvironment environment, StargateGraphqlContext context) {
    DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();

    EntityModel entityModel = model.getEntity();

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
    Query query =
        buildUpdateQuery(entityModel, modifiers, whereConditions, ifConditions, timestamp);

    List<TypedKeyValue> primaryKey = computePrimaryKey(entityModel, whereConditions);

    Function<List<MutationResult>, DataFetcherResult<Object>> resultBuilder =
        getDeleteOrUpdateResultBuilder(environment);

    return new MutationPayload<>(query, primaryKey, resultBuilder);
  }

  private Collection<ValueModifier> buildIncrementModifiers(
      List<IncrementModel> incrementModels,
      CqlKeyspaceDescribe keyspace,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument) {
    List<ValueModifier> modifiers = new ArrayList<>();

    for (IncrementModel incrementModel : incrementModels) {
      FieldModel column = incrementModel.getField();

      if (hasArgument.test(incrementModel.getArgumentName())) {
        Object graphqlValue = getArgument.apply(incrementModel.getArgumentName());
        Operation operation =
            incrementModel.isPrepend()
                ? Operation.PREPEND
                : Operation.APPEND; // handles both increment and append
        modifiers.add(
            ValueModifier.of(
                Target.column(column.getCqlName()),
                operation,
                Term.of(toCqlValue(graphqlValue, column.getCqlType(), keyspace))));
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
      CqlKeyspaceDescribe keyspace,
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

  private Query buildUpdateQuery(
      EntityModel entityModel,
      Collection<ValueModifier> modifiers,
      List<BuiltCondition> whereConditions,
      List<BuiltCondition> ifConditions,
      Optional<Long> timestamp) {
    return new QueryBuilder()
        .update(entityModel.getKeyspaceName(), entityModel.getCqlName())
        .ttl(model.getTtl().orElse(null))
        .timestamp(timestamp.orElse(null))
        .value(modifiers)
        .where(whereConditions)
        .ifs(ifConditions)
        .ifExists(model.ifExists())
        .parameters(buildParameters())
        .build();
  }
}
