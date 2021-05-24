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
package io.stargate.graphql.schema.graphqlfirst.processor;

import com.google.common.collect.ImmutableList;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.EntityListReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.EntityReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

abstract class OperationModelBuilderBase<T extends OperationModel> extends ModelBuilderBase<T> {

  protected final FieldDefinition operation;
  protected final String operationName;
  protected final Map<String, EntityModel> entities;
  protected final Map<String, ResponsePayloadModel> responsePayloads;

  protected OperationModelBuilderBase(
      FieldDefinition operation,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(context, operation.getSourceLocation());
    this.operation = operation;
    this.operationName = operation.getName();
    this.entities = entities;
    this.responsePayloads = responsePayloads;
  }

  OperationModel.ReturnType getReturnType(String operationDescription) throws SkipException {
    Type<?> graphqlType = TypeHelper.unwrapNonNull(operation.getType());

    if (graphqlType instanceof ListType) {
      Type<?> elementType = ((ListType) graphqlType).getType();
      elementType = TypeHelper.unwrapNonNull(elementType);
      if (elementType instanceof TypeName) {
        EntityModel entity = entities.get(((TypeName) elementType).getName());
        if (entity != null) {
          return new EntityListReturnType(entity);
        }
      }
    } else {
      assert graphqlType instanceof TypeName;
      String typeName = ((TypeName) graphqlType).getName();

      SimpleReturnType simple = SimpleReturnType.fromTypeName(typeName);
      if (simple != null) {
        return simple;
      }

      EntityModel entity = entities.get(typeName);
      if (entity != null) {
        return new EntityReturnType(entity);
      }

      ResponsePayloadModel payload = responsePayloads.get(typeName);
      if (payload != null) {
        return payload;
      }
    }
    invalidMapping(
        "%s: unsupported return type %s", operationDescription, TypeHelper.format(graphqlType));
    throw SkipException.INSTANCE;
  }

  /**
   * For each field of the given entity, try to find an operation argument of the same name, and
   * build a condition that will get appended to the CQL query.
   */
  protected List<ConditionModel> buildWhereConditions(EntityModel entity) throws SkipException {
    return buildConditions(
        entity,
        (inputValue, e) ->
            new WhereConditionModelBuilder(inputValue, operationName, e, entities, context),
        (value) -> DirectiveHelper.getDirective("cql_if", value).isPresent());
  }

  /**
   * For each field of the given entity, try to find an operation argument of the same name, and
   * build a condition that will get appended to the CQL query.
   */
  protected List<ConditionModel> buildIfConditions(EntityModel entity) throws SkipException {
    return buildConditions(
        entity,
        (inputValue, e) ->
            new IfConditionModelBuilder(inputValue, operationName, e, entities, context),
        (value) -> !DirectiveHelper.getDirective("cql_if", value).isPresent());
  }

  private <C extends ConditionModel> List<C> buildConditions(
      EntityModel entity,
      BiFunction<InputValueDefinition, EntityModel, ModelBuilderBase<C>> modelBuilder,
      Predicate<InputValueDefinition> skipIf)
      throws SkipException {
    ImmutableList.Builder<C> whereConditionsBuilder = ImmutableList.builder();
    boolean foundErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {
      if (skipIf.test(inputValue)) {
        // skip a field annotated with a given directive
        continue;
      }
      if (DirectiveHelper.getDirective("cql_pagingState", inputValue).isPresent()) {
        continue;
      }
      try {
        whereConditionsBuilder.add(modelBuilder.apply(inputValue, entity).build());
      } catch (SkipException __) {
        foundErrors = true;
      }
    }
    if (foundErrors) {
      throw SkipException.INSTANCE;
    }
    return whereConditionsBuilder.build();
  }

  protected void validateNoFiltering(List<ConditionModel> whereConditions, EntityModel entity)
      throws SkipException {
    Optional<String> maybeError = entity.validateNoFiltering(whereConditions);
    if (maybeError.isPresent()) {
      invalidMapping("Operation %s: %s", operationName, maybeError.get());
      throw SkipException.INSTANCE;
    }
  }
}
