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
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import java.util.Map;
import java.util.Optional;

class DirectiveModelsBuilder extends ModelBuilderBase<DirectiveModels> {

  /** The type of query, which determines how conditions are collected. */
  enum OperationType {
    /** cql_where or no directive => WHERE, cql_if => error. */
    SELECT,
    /** cql_where or no directive => WHERE, cql_if => IF. */
    DELETE,
    /**
     * cql_where or no directive on PK field => WHERE, cql_where on regular field => error, no
     * directive on regular field => ignore, cql_if => IF. cql_increment is allowed on the update as
     * the only one non-pk field.
     */
    UPDATE,
  }

  private static final String CQL_WHERE = "cql_where";
  private static final String CQL_IF = "cql_if";
  private static final String CQL_INCREMENT = "cql_increment";

  private final FieldDefinition operation;
  private final String operationName;
  private final OperationType operationType;
  private final EntityModel entity;
  private final Map<String, EntityModel> entities;

  DirectiveModelsBuilder(
      FieldDefinition operation,
      OperationType operationType,
      EntityModel entity,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(context, operation.getSourceLocation());
    this.operation = operation;
    this.operationName = operation.getName();
    this.operationType = operationType;
    this.entity = entity;
    this.entities = entities;
  }

  @Override
  DirectiveModels build() throws SkipException {
    ImmutableList.Builder<ConditionModel> ifConditions = ImmutableList.builder();
    ImmutableList.Builder<ConditionModel> whereConditions = ImmutableList.builder();
    ImmutableList.Builder<IncrementModel> incrementModels = ImmutableList.builder();

    boolean foundErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {

      if (DirectiveHelper.hasDirective(inputValue, "cql_pagingState")) {
        // It's a technical field that does not represent a condition
        continue;
      }

      boolean hasWhereDirective = DirectiveHelper.hasDirective(inputValue, CQL_WHERE);
      boolean hasIfDirective = DirectiveHelper.hasDirective(inputValue, CQL_IF);
      boolean hasCqlIncrementDirective = DirectiveHelper.hasDirective(inputValue, CQL_INCREMENT);

      foundErrors =
          validateIfHasErrors(
              inputValue, hasWhereDirective, hasIfDirective, hasCqlIncrementDirective);
      if (foundErrors) {
        continue;
      }

      Optional<FieldModel> maybeField =
          findField(inputValue, hasIfDirective, hasCqlIncrementDirective);
      if (!maybeField.isPresent()) {
        foundErrors = true;
        continue;
      }
      FieldModel field = maybeField.get();

      boolean isWhereCondition;

      switch (operationType) {
        case SELECT:
          if (hasIfDirective) {
            invalidMapping(
                "Operation %s: @%s is not allowed on query arguments (%s)",
                operationName, CQL_IF, inputValue.getName());
            foundErrors = true;
            continue;
          }
          isWhereCondition = true;
          break;
        case DELETE:
          isWhereCondition = !hasIfDirective;
          break;
        case UPDATE:
          if (field.isPrimaryKey()) {
            if (hasIfDirective) {
              reportInvalidMapping(inputValue, CQL_IF);
              foundErrors = true;
              continue;
            }
            if (hasCqlIncrementDirective) {
              reportInvalidMapping(inputValue, CQL_INCREMENT);
              foundErrors = true;
              continue;
            }
            isWhereCondition = true;
          } else {
            if (hasWhereDirective) {
              invalidMapping(
                  "Mutation %s: @%s is only allowed on primary key fields for updates (%s)",
                  operationName, CQL_WHERE, inputValue.getName());
              foundErrors = true;
              continue;
            }
            if (!hasIfDirective && !hasCqlIncrementDirective) {
              // Ignore non-annotated regular field (it's an update value, not a condition)
              continue;
            }
            isWhereCondition = false;
          }
          break;
        default:
          throw new AssertionError();
      }

      if (isWhereCondition) {
        whereConditions.add(
            new WhereConditionModelBuilder(
                    inputValue, operationName, entity, field, entities, context)
                .build());
      } else if (hasCqlIncrementDirective) {
        incrementModels.add(
            new IncrementModelBuilder(inputValue, operationName, entity, field, entities, context)
                .build());
      } else {
        ifConditions.add(
            new IfConditionModelBuilder(inputValue, operationName, entity, field, entities, context)
                .build());
      }
    }
    if (foundErrors) {
      throw SkipException.INSTANCE;
    }
    return new DirectiveModels(
        ifConditions.build(), whereConditions.build(), incrementModels.build());
  }

  private boolean validateIfHasErrors(
      InputValueDefinition inputValue,
      boolean hasWhereDirective,
      boolean hasIfDirective,
      boolean hasCqlIncrementDirective) {
    if (hasWhereDirective && hasIfDirective) {
      reportTwoAnnotationsSet(inputValue, CQL_WHERE, CQL_IF);
      return true;
    }

    if (hasWhereDirective && hasCqlIncrementDirective) {
      reportTwoAnnotationsSet(inputValue, CQL_WHERE, CQL_INCREMENT);
      return true;
    }

    if (hasIfDirective && hasCqlIncrementDirective) {
      reportTwoAnnotationsSet(inputValue, CQL_IF, CQL_INCREMENT);
      return true;
    }
    return false;
  }

  private void reportTwoAnnotationsSet(
      InputValueDefinition inputValue, String firstDirective, String secondDirective) {
    invalidMapping(
        "Operation %s: can't set both @%s and @%s on argument %s",
        operationName, firstDirective, secondDirective, inputValue.getName());
  }

  private void reportInvalidMapping(InputValueDefinition inputValue, String directiveName) {
    invalidMapping(
        "Mutation %s: @%s is not allowed on primary key fields (%s)",
        operationName, directiveName, inputValue.getName());
  }

  private Optional<FieldModel> findField(
      InputValueDefinition inputValue, boolean hasIfDirective, boolean hasCqlIncrementDirective) {
    String directiveName;
    if (hasIfDirective) {
      directiveName = CQL_IF;
    } else if (hasCqlIncrementDirective) {
      directiveName = CQL_INCREMENT;
    } else {
      directiveName = CQL_WHERE;
    }
    Optional<Directive> directive = DirectiveHelper.getDirective(directiveName, inputValue);
    String fieldName =
        directive
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "field", context))
            .orElse(inputValue.getName());
    Optional<FieldModel> result =
        entity.getAllColumns().stream()
            .filter(f -> f.getGraphqlName().equals(fieldName))
            .findFirst();
    if (!result.isPresent()) {
      invalidMapping(
          "Operation %s: could not find field %s in type %s",
          operationName, fieldName, entity.getGraphqlName());
    }
    return result;
  }
}
