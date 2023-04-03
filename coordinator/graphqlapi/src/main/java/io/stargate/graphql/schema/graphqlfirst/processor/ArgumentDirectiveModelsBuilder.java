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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    ImmutableList.Builder<ConditionModel> whereConditions = ImmutableList.builder();
    ImmutableList.Builder<ConditionModel> ifConditions = ImmutableList.builder();
    ImmutableList.Builder<IncrementModel> increments = ImmutableList.builder();

    boolean hasErrors = false;
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {
      try {
        buildArgument(inputValue, whereConditions, ifConditions, increments);
      } catch (SkipException e) {
        // We'll fail eventually, but keep processing other arguments in case there are more errors
        // to report
        hasErrors = true;
      }
    }
    if (hasErrors) {
      throw SkipException.INSTANCE;
    }
    return new ArgumentDirectiveModels(
        ifConditions.build(), whereConditions.build(), increments.build());
  }

  private void buildArgument(
      InputValueDefinition inputValue,
      ImmutableList.Builder<ConditionModel> whereConditions,
      ImmutableList.Builder<ConditionModel> ifConditions,
      ImmutableList.Builder<IncrementModel> increments)
      throws SkipException {

    Set<Directive> directives =
        collectDirectives(
            inputValue,
            CqlDirectives.PAGING_STATE,
            CqlDirectives.WHERE,
            CqlDirectives.IF,
            CqlDirectives.INCREMENT,
            CqlDirectives.TIMESTAMP);
    if (directives.size() > 1) {
      reportTooManyDirectives(directives, inputValue);
      throw SkipException.INSTANCE;
    }

    Optional<Directive> directive = directives.stream().findFirst();
    if (is(directive, CqlDirectives.PAGING_STATE) || is(directive, CqlDirectives.TIMESTAMP)) {
      // It's a technical field that does not represent a condition, ignore it
      return;
    }

    FieldModel field = findField(inputValue, directive);

    switch (operationType) {
      case SELECT:
        buildSelectArgument(inputValue, directive, field, whereConditions);
        break;
      case DELETE:
        buildDeleteArgument(inputValue, directive, field, whereConditions, ifConditions);
        break;
      case UPDATE:
        buildUpdateArgument(
            inputValue, directive, field, whereConditions, ifConditions, increments);
        break;
      default:
        throw new AssertionError("Unknown operation type " + operationType);
    }
  }

  private Set<Directive> collectDirectives(
      InputValueDefinition inputValue, String... directiveNames) {
    ImmutableSet.Builder<Directive> result = ImmutableSet.builder();
    for (String directiveName : directiveNames) {
      DirectiveHelper.getDirective(directiveName, inputValue).ifPresent(result::add);
    }
    return result.build();
  }

  private boolean is(Optional<Directive> directive, String directiveName) {
    return directive.filter(d -> d.getName().equals(directiveName)).isPresent();
  }

  private FieldModel findField(InputValueDefinition inputValue, Optional<Directive> directive)
      throws SkipException {
    String fieldName =
        directive
            .flatMap(
                d ->
                    DirectiveHelper.getStringArgument(
                        d, CqlDirectives.WHERE_OR_IF_OR_INCREMENT_FIELD, context))
            .orElse(inputValue.getName());
    return entity.getAllColumns().stream()
        .filter(f -> f.getGraphqlName().equals(fieldName))
        .findFirst()
        .orElseThrow(
            () -> {
              invalidMapping(
                  "Operation %s: could not find field %s in type %s",
                  operationName, fieldName, entity.getGraphqlName());
              return SkipException.INSTANCE;
            });
  }

  private void buildSelectArgument(
      InputValueDefinition inputValue,
      Optional<Directive> directive,
      FieldModel field,
      ImmutableList.Builder<ConditionModel> whereConditions)
      throws SkipException {

    if (is(directive, CqlDirectives.IF) || is(directive, CqlDirectives.INCREMENT)) {
      reportDirectiveNotAllowed(inputValue, directive, "SELECT arguments");
    }

    whereConditions.add(newWhereCondition(inputValue, directive, field));
  }

  private void buildDeleteArgument(
      InputValueDefinition inputValue,
      Optional<Directive> directive,
      FieldModel field,
      ImmutableList.Builder<ConditionModel> whereConditions,
      ImmutableList.Builder<ConditionModel> ifConditions)
      throws SkipException {

    if (is(directive, CqlDirectives.INCREMENT)) {
      reportDirectiveNotAllowed(inputValue, directive, "DELETE arguments");
    }

    if (is(directive, CqlDirectives.IF)) {
      ifConditions.add(newIfCondition(inputValue, directive, field));
    } else {
      whereConditions.add(newWhereCondition(inputValue, directive, field));
    }
  }

  private void buildUpdateArgument(
      InputValueDefinition inputValue,
      Optional<Directive> directive,
      FieldModel field,
      ImmutableList.Builder<ConditionModel> whereConditions,
      ImmutableList.Builder<ConditionModel> ifConditions,
      ImmutableList.Builder<IncrementModel> increments)
      throws SkipException {
    if (field.isPrimaryKey()) {
      if (is(directive, CqlDirectives.IF) || is(directive, CqlDirectives.INCREMENT)) {
        reportDirectiveNotAllowed(inputValue, directive, "UPDATE primary key arguments");
        throw SkipException.INSTANCE;
      }
      whereConditions.add(newWhereCondition(inputValue, directive, field));
    } else {
      if (is(directive, CqlDirectives.WHERE)) {
        reportDirectiveNotAllowed(inputValue, directive, "UPDATE non-primary key arguments");
        throw SkipException.INSTANCE;
      } else if (is(directive, CqlDirectives.IF)) {
        ifConditions.add(newIfCondition(inputValue, directive, field));
      } else if (is(directive, CqlDirectives.INCREMENT)) {
        increments.add(newIncrement(inputValue, directive, field));
      }
      // otherwise it's an update value (non-annotated regular field), not a condition => ignore it
    }
  }

  private ConditionModel newWhereCondition(
      InputValueDefinition inputValue, Optional<Directive> directive, FieldModel field)
      throws SkipException {
    return new WhereConditionModelBuilder(
            inputValue, directive, entity, field, operationName, entities, context)
        .build();
  }

  private ConditionModel newIfCondition(
      InputValueDefinition inputValue, Optional<Directive> directive, FieldModel field)
      throws SkipException {
    return new IfConditionModelBuilder(
            inputValue, directive, entity, field, operationName, entities, context)
        .build();
  }

  private IncrementModel newIncrement(
      InputValueDefinition inputValue, Optional<Directive> directive, FieldModel field)
      throws SkipException {
    return new IncrementModelBuilder(
            inputValue, directive, entity, field, operationName, entities, context)
        .build();
  }

  private void reportTooManyDirectives(Set<Directive> directives, InputValueDefinition inputValue) {
    invalidMapping(
        "Operation %s: argument %s can only use one of %s",
        operationName,
        inputValue.getName(),
        directives.stream().map(d -> "@" + d.getName()).collect(Collectors.joining(",")));
  }

  private void reportDirectiveNotAllowed(
      InputValueDefinition inputValue,
      Optional<Directive> directive,
      String inputValueDescription) {
    assert directive.isPresent();
    invalidMapping(
        "Operation %s: @%s is not allowed on %s (%s)",
        operationName, directive.get().getName(), inputValueDescription, inputValue.getName());
  }
}
