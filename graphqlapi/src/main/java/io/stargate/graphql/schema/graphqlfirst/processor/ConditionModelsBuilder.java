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

class ConditionModelsBuilder extends ModelBuilderBase<ConditionModels> {

  /** The type of query, which determines how conditions are collected. */
  enum OperationType {
    /** cql_where or no directive => WHERE, cql_if => error. */
    SELECT,
    /** cql_where or no directive => WHERE, cql_if => IF. */
    DELETE,
    /**
     * cql_where or no directive on PK field => WHERE, cql_where on regular field => error, no
     * directive on regular field => ignore, cql_if => IF.
     */
    UPDATE,
  }

  private static final String CQL_WHERE = "cql_where";
  private static final String CQL_IF = "cql_if";

  private final FieldDefinition operation;
  private final String operationName;
  private final OperationType operationType;
  private final EntityModel entity;
  private final Map<String, EntityModel> entities;

  ConditionModelsBuilder(
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
  ConditionModels build() throws SkipException {
    ImmutableList.Builder<ConditionModel> ifConditions = ImmutableList.builder();
    ImmutableList.Builder<ConditionModel> whereConditions = ImmutableList.builder();
    boolean foundErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {

      if (hasDirective(inputValue, "cql_pagingState")) {
        // It's a technical field that does not represent a condition
        continue;
      }

      boolean hasWhereDirective = hasDirective(inputValue, CQL_WHERE);
      boolean hasIfDirective = hasDirective(inputValue, CQL_IF);

      if (hasWhereDirective && hasIfDirective) {
        invalidMapping(
            "Operation %s: can't set both @%s and @%s on argument %s",
            operationName, CQL_WHERE, CQL_IF, inputValue.getName());
        foundErrors = true;
        continue;
      }

      Optional<FieldModel> maybeField = findField(inputValue, hasIfDirective);
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
              invalidMapping(
                  "Mutation %s: @%s is not allowed on primary key fields (%s)",
                  operationName, CQL_IF, inputValue.getName());
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
            if (!hasIfDirective) {
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
      } else {
        ifConditions.add(
            new IfConditionModelBuilder(inputValue, operationName, entity, field, entities, context)
                .build());
      }
    }
    if (foundErrors) {
      throw SkipException.INSTANCE;
    }
    return new ConditionModels(ifConditions.build(), whereConditions.build());
  }

  private boolean hasDirective(InputValueDefinition inputValue, String directive) {
    return DirectiveHelper.getDirective(directive, inputValue).isPresent();
  }

  private Optional<FieldModel> findField(InputValueDefinition inputValue, boolean hasIfDirective) {
    Optional<Directive> directive =
        DirectiveHelper.getDirective(hasIfDirective ? CQL_IF : CQL_WHERE, inputValue);
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
