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
  private final FieldModel field;
  protected final Map<String, EntityModel> entities;

  protected ConditionModelBuilderBase(
      InputValueDefinition argument,
      String operationName,
      EntityModel entity,
      FieldModel field,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(context, argument.getSourceLocation());
    this.argument = argument;
    this.operationName = operationName;
    this.entity = entity;
    this.field = field;
    this.entities = entities;
  }

  protected abstract String getDirectiveName();

  protected abstract void validate(FieldModel field, Predicate predicate) throws SkipException;

  @Override
  protected ConditionModel build() throws SkipException {

    Optional<Directive> directive = DirectiveHelper.getDirective(getDirectiveName(), argument);
    Predicate predicate =
        directive
            .flatMap(
                d ->
                    DirectiveHelper.getEnumArgument(
                        d, CqlDirectives.WHERE_OR_IF_PREDICATE, Predicate.class, context))
            .orElse(Predicate.EQ);

    validate(field, predicate);

    return new ConditionModel(field, predicate, argument.getName());
  }

  protected void checkArgumentIsSameAs(FieldModel field) throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    Type<?> fieldInputType =
        toInput(field.getGraphqlType(), argument, entity, field, entities, operationName);

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

  protected void checkArgumentIsListOf(FieldModel field) throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    Type<?> fieldInputType =
        toInput(field.getGraphqlType(), argument, entity, field, entities, operationName);
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
