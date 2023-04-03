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

import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;

public abstract class ArgumentDirectiveModelBuilderBase<ModelT> extends ModelBuilderBase<ModelT> {
  protected final InputValueDefinition argument;
  protected final Optional<Directive> directive;
  protected final EntityModel entity;
  protected final FieldModel field;
  protected final String operationName;
  protected final Map<String, EntityModel> entities;

  protected ArgumentDirectiveModelBuilderBase(
      InputValueDefinition argument,
      Optional<Directive> directive,
      EntityModel entity,
      FieldModel field,
      String operationName,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(context, argument.getSourceLocation());
    this.argument = argument;
    this.directive = directive;
    this.entity = entity;
    this.field = field;
    this.operationName = operationName;
    this.entities = entities;
  }

  protected Type<?> fieldInputType() throws SkipException {
    try {
      return TypeHelper.toInput(TypeHelper.unwrapNonNull(field.getGraphqlType()), entities);
    } catch (IllegalArgumentException e) {
      invalidMapping(
          "Operation %s: can't infer expected input type for %s (matching %s.%s) because %s",
          operationName,
          argument.getName(),
          entity.getGraphqlName(),
          field.getGraphqlName(),
          e.getMessage());
      throw SkipException.INSTANCE;
    }
  }
}
