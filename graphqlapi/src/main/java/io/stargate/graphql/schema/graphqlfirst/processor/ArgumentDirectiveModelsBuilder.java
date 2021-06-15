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

class ArgumentDirectiveModelsBuilder extends ModelBuilderBase<ArgumentDirectiveModels> {

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

  private final FieldDefinition operation;
  private final String operationName;
  private final OperationType operationType;
  private final EntityModel entity;
  private final Map<String, EntityModel> entities;

  ArgumentDirectiveModelsBuilder(
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
  ArgumentDirectiveModels build() throws SkipException {
    ImmutableList.Builder<ConditionModel> ifConditions = ImmutableList.builder();
    ImmutableList.Builder<ConditionModel> whereConditions = ImmutableList.builder();
    ImmutableList.Builder<IncrementModel> incrementModels = ImmutableList.builder();

    BuildState buildState = new BuildState();
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {

      if (DirectiveHelper.hasDirective(inputValue, CqlDirectives.PAGING_STATE)) {
        // It's a technical field that does not represent a condition
        continue;
      }

      boolean hasWhereDirective = DirectiveHelper.hasDirective(inputValue, CqlDirectives.WHERE);
      boolean hasIfDirective = DirectiveHelper.hasDirective(inputValue, CqlDirectives.IF);
      boolean hasCqlIncrementDirective =
          DirectiveHelper.hasDirective(inputValue, CqlDirectives.INCREMENT);

      buildState.setFoundErrors(
          validateIfHasErrors(
              inputValue, hasWhereDirective, hasIfDirective, hasCqlIncrementDirective));
      if (buildState.foundErrors) {
        continue;
      }

      Optional<FieldModel> maybeField =
          findField(inputValue, hasIfDirective, hasCqlIncrementDirective);
      if (!maybeField.isPresent()) {
        buildState.setFoundErrors();
        continue;
      }
      FieldModel field = maybeField.get();

      switch (operationType) {
        case SELECT:
          if (hasIfDirective) {
            queryNotAllowed(buildState, inputValue, CqlDirectives.IF);
            continue;
          }
          if (hasCqlIncrementDirective) {
            queryNotAllowed(buildState, inputValue, CqlDirectives.INCREMENT);
            continue;
          }
          buildState.setWhereCondition();
          break;
        case DELETE:
          buildState.setWhereCondition(!hasIfDirective);

          if (hasCqlIncrementDirective) {
            deleteNotAllowed(buildState, inputValue, CqlDirectives.INCREMENT);
            continue;
          }
          break;
        case UPDATE:
          if (!handleUpdateAndReturnStatus(
              buildState,
              inputValue,
              hasWhereDirective,
              hasIfDirective,
              hasCqlIncrementDirective,
              field)) {
            continue;
          }
          break;
        default:
          throw new AssertionError();
      }

      buildDirectives(
          ifConditions,
          whereConditions,
          incrementModels,
          buildState,
          inputValue,
          hasCqlIncrementDirective,
          field);
    }
    if (buildState.isFoundErrors()) {
      throw SkipException.INSTANCE;
    }
    return new ArgumentDirectiveModels(
        ifConditions.build(), whereConditions.build(), incrementModels.build());
  }

  private void buildDirectives(
      ImmutableList.Builder<ConditionModel> ifConditions,
      ImmutableList.Builder<ConditionModel> whereConditions,
      ImmutableList.Builder<IncrementModel> incrementModels,
      BuildState buildState,
      InputValueDefinition inputValue,
      boolean hasCqlIncrementDirective,
      FieldModel field)
      throws SkipException {
    if (buildState.isWhereCondition()) {
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

  // returns true if the update was handled, otherwise false
  private boolean handleUpdateAndReturnStatus(
      BuildState buildState,
      InputValueDefinition inputValue,
      boolean hasWhereDirective,
      boolean hasIfDirective,
      boolean hasCqlIncrementDirective,
      FieldModel field) {
    if (field.isPrimaryKey()) {
      if (hasIfDirective) {
        reportInvalidMapping(inputValue, CqlDirectives.IF);
        buildState.setFoundErrors();
        return false;
      }
      if (hasCqlIncrementDirective) {
        reportInvalidMapping(inputValue, CqlDirectives.INCREMENT);
        buildState.setFoundErrors();
        return false;
      }
      buildState.setWhereCondition();
    } else {
      if (hasWhereDirective) {
        invalidMapping(
            "Mutation %s: @%s is only allowed on primary key fields for updates (%s)",
            operationName, CqlDirectives.WHERE, inputValue.getName());
        buildState.setFoundErrors();
        return false;
      }
      if (!hasIfDirective && !hasCqlIncrementDirective) {
        // Ignore non-annotated regular field (it's an update value, not a condition)
        return false;
      }
      buildState.setWhereCondition(false);
    }
    return true;
  }

  private boolean validateIfHasErrors(
      InputValueDefinition inputValue,
      boolean hasWhereDirective,
      boolean hasIfDirective,
      boolean hasCqlIncrementDirective) {
    if (hasWhereDirective && hasIfDirective) {
      reportTwoAnnotationsSet(inputValue, CqlDirectives.WHERE, CqlDirectives.IF);
      return true;
    }

    if (hasWhereDirective && hasCqlIncrementDirective) {
      reportTwoAnnotationsSet(inputValue, CqlDirectives.WHERE, CqlDirectives.INCREMENT);
      return true;
    }

    if (hasIfDirective && hasCqlIncrementDirective) {
      reportTwoAnnotationsSet(inputValue, CqlDirectives.IF, CqlDirectives.INCREMENT);
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

  private void deleteNotAllowed(
      BuildState buildState, InputValueDefinition inputValue, String cqlDirective) {
    operationNotAllowed(buildState, inputValue, cqlDirective, "delete");
  }

  private void queryNotAllowed(
      BuildState buildState, InputValueDefinition inputValue, String cqlDirective) {
    operationNotAllowed(buildState, inputValue, cqlDirective, "query");
  }

  private void operationNotAllowed(
      BuildState buildState,
      InputValueDefinition inputValue,
      String cqlDirective,
      String queryType) {
    invalidMapping(
        "Operation %s: @%s is not allowed on %s arguments (%s)",
        operationName, cqlDirective, queryType, inputValue.getName());
    buildState.setFoundErrors();
  }

  private Optional<FieldModel> findField(
      InputValueDefinition inputValue, boolean hasIfDirective, boolean hasCqlIncrementDirective) {
    String directiveName;
    if (hasIfDirective) {
      directiveName = CqlDirectives.IF;
    } else if (hasCqlIncrementDirective) {
      directiveName = CqlDirectives.INCREMENT;
    } else {
      directiveName = CqlDirectives.WHERE;
    }
    Optional<Directive> directive = DirectiveHelper.getDirective(directiveName, inputValue);
    String fieldName =
        directive
            .flatMap(
                d ->
                    DirectiveHelper.getStringArgument(
                        d, CqlDirectives.WHERE_OR_IF_OR_INCREMENT_FIELD, context))
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

  static class BuildState {

    private boolean foundErrors = false;
    private boolean isWhereCondition = false;

    public boolean isFoundErrors() {
      return foundErrors;
    }

    public void setFoundErrors() {
      this.foundErrors = true;
    }

    public void setFoundErrors(boolean foundErrors) {
      this.foundErrors = foundErrors;
    }

    public boolean isWhereCondition() {
      return isWhereCondition;
    }

    public void setWhereCondition(boolean whereCondition) {
      isWhereCondition = whereCondition;
    }

    public void setWhereCondition() {
      isWhereCondition = true;
    }
  }
}
