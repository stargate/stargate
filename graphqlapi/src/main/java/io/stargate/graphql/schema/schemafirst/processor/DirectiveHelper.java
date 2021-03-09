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
package io.stargate.graphql.schema.schemafirst.processor;

import graphql.language.Argument;
import graphql.language.BooleanValue;
import graphql.language.Directive;
import graphql.language.DirectivesContainer;
import graphql.language.EnumValue;
import graphql.language.StringValue;
import graphql.language.Value;
import java.util.Optional;
import java.util.function.Function;

/** Helper methods to read directives as we traverse the GraphQL AST. */
class DirectiveHelper {

  static Optional<Directive> getDirective(String name, DirectivesContainer<?> container) {
    if (container.hasDirective(name)) {
      return Optional.of(container.getDirectives(name).get(0));
    }
    return Optional.empty();
  }

  static Optional<String> getStringArgument(
      Optional<Directive> directive, String argumentName, ProcessingContext context) {
    return getArgument(
        directive, argumentName, StringValue.class, v -> Optional.of(v.getValue()), context);
  }

  static Optional<Boolean> getBooleanArgument(
      Optional<Directive> directive, String argumentName, ProcessingContext context) {
    return getArgument(
        directive, argumentName, BooleanValue.class, v -> Optional.of(v.isValue()), context);
  }

  static <EnumT extends Enum<EnumT>> Optional<EnumT> getEnumArgument(
      Optional<Directive> directive,
      String argumentName,
      Class<EnumT> enumClass,
      ProcessingContext context) {
    return getArgument(
        directive,
        argumentName,
        EnumValue.class,
        v -> {
          try {
            return Optional.of(Enum.valueOf(enumClass, v.getName()));
          } catch (IllegalArgumentException e) {
            context.addError(
                directive.map(Directive::getSourceLocation).orElse(null),
                ProcessingMessageType.InvalidSyntax,
                "Invalid value '%s' for argument %s: not an enum constant",
                v.getName(),
                argumentName);
            return Optional.empty();
          }
        },
        context);
  }

  static <ResultT, ValueT extends Value<ValueT>> Optional<ResultT> getArgument(
      Optional<Directive> directive,
      String argumentName,
      Class<ValueT> valueClass,
      Function<ValueT, Optional<ResultT>> extractor,
      ProcessingContext context) {
    return directive
        .flatMap(d -> Optional.ofNullable(d.getArgument(argumentName)))
        .map(Argument::getValue)
        .filter(
            v -> {
              if (valueClass.isInstance(v)) {
                return true;
              } else {
                context.addError(
                    directive.map(Directive::getSourceLocation).orElse(null),
                    ProcessingMessageType.InvalidSyntax,
                    "Invalid value for argument %s: expected a %s",
                    argumentName,
                    valueClass.getSimpleName());
                return false;
              }
            })
        .flatMap(v -> extractor.apply(valueClass.cast(v)));
  }

  private DirectiveHelper() {
    // hide constructor for utility class
  }
}
