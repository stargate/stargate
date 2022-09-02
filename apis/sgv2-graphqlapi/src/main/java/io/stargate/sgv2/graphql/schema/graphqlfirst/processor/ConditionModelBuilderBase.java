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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import static graphql.language.ListType.newListType;

import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;

public abstract class ConditionModelBuilderBase
    extends ArgumentDirectiveModelBuilderBase<ConditionModel> {

  protected ConditionModelBuilderBase(
      InputValueDefinition argument,
      Optional<Directive> directive,
      EntityModel entity,
      FieldModel field,
      String operationName,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(argument, directive, entity, field, operationName, entities, context);
  }

  protected abstract void validate(FieldModel field, Predicate predicate) throws SkipException;

  @Override
  protected ConditionModel build() throws SkipException {

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
    Type<?> fieldInputType = fieldInputType();

    if (!TypeHelper.deepEquals(argumentType, fieldInputType)) {
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
    Type<?> fieldInputType = fieldInputType();
    Type<?> expectedArgumentType = newListType(fieldInputType).build();

    if (!TypeHelper.deepEquals(argumentType, expectedArgumentType)) {
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
