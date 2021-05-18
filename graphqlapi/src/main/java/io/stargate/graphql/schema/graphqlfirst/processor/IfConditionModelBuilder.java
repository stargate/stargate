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

import static graphql.language.ListType.newListType;

import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import io.stargate.db.query.Predicate;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;

class IfConditionModelBuilder extends ModelBuilderBase<IfConditionModel> {

  private final InputValueDefinition argument;
  private final String operationName;
  private final EntityModel entity;
  private final Map<String, EntityModel> entities;

  IfConditionModelBuilder(
      InputValueDefinition argument,
      String operationName,
      EntityModel entity,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(context, argument.getSourceLocation());
    this.argument = argument;
    this.operationName = operationName;
    this.entity = entity;
    this.entities = entities;
  }

  IfConditionModel build() throws SkipException {

    Optional<Directive> ifDirective = DirectiveHelper.getDirective("cql_if", argument);
    String fieldName =
        ifDirective
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "field", context))
            .orElse(argument.getName());
    Predicate predicate =
        ifDirective
            .flatMap(d -> DirectiveHelper.getEnumArgument(d, "predicate", Predicate.class, context))
            .orElse(Predicate.EQ);

    FieldModel field = findField(fieldName);

    // The CQL IF works only for regular columns (non PK, CK)
    if (field.isPartitionKey()) {
      invalidMapping(
          "Operation %s: predicate %s is not supported for partition keys (field %s)",
          operationName, predicate, field.getGraphqlName());
    }
    if (field.isClusteringColumn()) {
      invalidMapping(
          "Operation %s: predicate %s is not supported for clustering keys (field %s)",
          operationName, predicate, field.getGraphqlName());
    } else {
      checkValidForRegularColumn(predicate, field);
    }

    return new IfConditionModel(field, predicate, argument.getName());
  }

  private FieldModel findField(String fieldName) throws SkipException {
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

  private void checkValidForRegularColumn(Predicate predicate, FieldModel field)
      throws SkipException {
    switch (predicate) {
      case EQ:
      case NEQ:
      case LT:
      case GT:
      case LTE:
      case GTE:
        checkArgumentIsSameAs(field);
        break;
      case IN:
        checkArgumentIsListOf(field);
        break;
      case CONTAINS:
        checkArgumentIsElementOf(field);
      default:
        invalidMapping(
            "Operation %s: predicate %s is not supported for clustering field %s "
                + "(expected EQ, LT, GT, LTE, GTE or IN)",
            operationName, predicate, field.getGraphqlName());
        throw SkipException.INSTANCE;
    }
  }

  private void checkArgumentIsSameAs(FieldModel field) throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    Type<?> fieldInputType = toInput(field.getGraphqlType(), argument, entity, field);

    if (!argumentType.isEqualTo(fieldInputType)) {
      invalidMapping(
          "Operation %s: expected argument %s to have type %s to match %s.%s",
          operationName,
          argument.getName(),
          TypeHelper.format(fieldInputType),
          entity.getGraphqlName(),
          field.getGraphqlName());
      throw SkipException.INSTANCE;
    }
  }

  private void checkArgumentIsListOf(FieldModel field) throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    Type<?> fieldInputType = toInput(field.getGraphqlType(), argument, entity, field);
    Type<?> expectedArgumentType = newListType(fieldInputType).build();

    if (!argumentType.isEqualTo(expectedArgumentType)) {
      invalidMapping(
          "Operation %s: expected argument %s to have type %s to match %s.%s",
          operationName,
          argument.getName(),
          TypeHelper.format(expectedArgumentType),
          entity.getGraphqlName(),
          field.getGraphqlName());
    }
  }

  private void checkArgumentIsElementOf(FieldModel field) throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    Type<?> fieldInputType = toInput(field.getGraphqlType(), argument, entity, field);
    if (!(fieldInputType instanceof ListType)) {
      invalidMapping(
          "Operation %s: CONTAINS predicate cannot be used with argument %s "
              + "because it is not a list",
          operationName, argument.getName());
      throw SkipException.INSTANCE;
    }
    Type<?> expectedArgumentType = ((ListType) fieldInputType).getType();

    if (!argumentType.isEqualTo(expectedArgumentType)) {
      invalidMapping(
          "Operation %s: expected argument %s to have type %s to match element type of %s.%s",
          operationName,
          argument.getName(),
          TypeHelper.format(expectedArgumentType),
          entity.getGraphqlName(),
          field.getGraphqlName());
    }
  }

  private Type<?> toInput(
      Type<?> fieldType, InputValueDefinition inputValue, EntityModel entity, FieldModel field)
      throws SkipException {
    try {
      return TypeHelper.toInput(TypeHelper.unwrapNonNull(fieldType), entities);
    } catch (IllegalArgumentException e) {
      invalidMapping(
          "Operation %s: can't infer expected input type for %s (matching %s.%s) because %s",
          operationName,
          inputValue.getName(),
          entity.getGraphqlName(),
          field.getGraphqlName(),
          e.getMessage());
      throw SkipException.INSTANCE;
    }
  }
}
