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
import graphql.language.Type;
import io.stargate.db.query.Predicate;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;

public abstract class ConditionModelBuilderBase extends ModelBuilderBase<ConditionModel> {
  protected final InputValueDefinition argument;
  protected final String operationName;
  protected final EntityModel entity;
  protected final Map<String, EntityModel> entities;

  protected ConditionModelBuilderBase(
      ProcessingContext context,
      InputValueDefinition argument,
      String operationName,
      EntityModel entity,
      Map<String, EntityModel> entities) {
    super(context, argument.getSourceLocation());
    this.argument = argument;
    this.operationName = operationName;
    this.entity = entity;
    this.entities = entities;
  }

  @Override
  protected ConditionModel build() throws SkipException {

    Optional<Directive> directive = DirectiveHelper.getDirective(getDirectiveName(), argument);
    String fieldName =
        directive
            .flatMap(d -> DirectiveHelper.getStringArgument(d, "field", context))
            .orElse(argument.getName());
    Predicate predicate =
        directive
            .flatMap(d -> DirectiveHelper.getEnumArgument(d, "predicate", Predicate.class, context))
            .orElse(Predicate.EQ);

    FieldModel field = findField(fieldName);

    validate(field, predicate);

    return new ConditionModel(field, predicate, argument.getName());
  }

  protected void checkArgumentIsSameAs(FieldModel field) throws SkipException {

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

  protected Type<?> toInput(
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

  protected FieldModel findField(String fieldName) throws SkipException {
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

  protected abstract String getDirectiveName();

  protected abstract void validate(FieldModel field, Predicate predicate) throws SkipException;

  protected void checkArgumentIsListOf(FieldModel field) throws SkipException {

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
}
