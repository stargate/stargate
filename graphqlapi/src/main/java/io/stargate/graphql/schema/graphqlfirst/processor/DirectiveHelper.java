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

import graphql.language.Argument;
import graphql.language.BooleanValue;
import graphql.language.Directive;
import graphql.language.DirectivesContainer;
import graphql.language.EnumValue;
import graphql.language.InputValueDefinition;
import graphql.language.IntValue;
import graphql.language.StringValue;
import graphql.language.Value;
import java.util.Optional;
import java.util.function.Function;

/** Helper methods to read directives as we traverse the GraphQL AST. */
class DirectiveHelper {

  /** Note: this assumes that the directive is not repeatable. */
  static Optional<Directive> getDirective(String name, DirectivesContainer<?> container) {
    if (container.hasDirective(name)) {
      return Optional.of(container.getDirectives(name).get(0));
    }
    return Optional.empty();
  }

  public static boolean hasDirective(InputValueDefinition inputValue, String directive) {
    return getDirective(directive, inputValue).isPresent();
  }

  static Optional<String> getStringArgument(
      Directive directive, String argumentName, ProcessingContext context) {
    return getArgument(
        directive, argumentName, StringValue.class, v -> Optional.of(v.getValue()), context);
  }

  static Optional<Boolean> getBooleanArgument(
      Directive directive, String argumentName, ProcessingContext context) {
    return getArgument(
        directive, argumentName, BooleanValue.class, v -> Optional.of(v.isValue()), context);
  }

  static Optional<Integer> getIntArgument(
      Directive directive, String argumentName, ProcessingContext context) {
    return getArgument(
        directive,
        argumentName,
        IntValue.class,
        v -> {
          try {
            return Optional.of(v.getValue().intValueExact());
          } catch (ArithmeticException e) {
            context.addError(
                directive.getSourceLocation(),
                ProcessingErrorType.InvalidSyntax,
                "%s.%s: value '%s' out of range, expected a 32-bit signed integer",
                directive.getName(),
                argumentName,
                v.getValue());
            return Optional.empty();
          }
        },
        context);
  }

  static <EnumT extends Enum<EnumT>> Optional<EnumT> getEnumArgument(
      Directive directive, String argumentName, Class<EnumT> enumClass, ProcessingContext context) {
    return getArgument(
        directive,
        argumentName,
        EnumValue.class,
        v -> {
          try {
            return Optional.of(Enum.valueOf(enumClass, v.getName()));
          } catch (IllegalArgumentException e) {
            context.addError(
                directive.getSourceLocation(),
                ProcessingErrorType.InvalidSyntax,
                "%s.%s: invalid value '%s', must be an enum constant",
                directive.getName(),
                argumentName,
                v.getName());
            return Optional.empty();
          }
        },
        context);
  }

  static <ResultT, ValueT extends Value<ValueT>> Optional<ResultT> getArgument(
      Directive directive,
      String argumentName,
      Class<ValueT> valueClass,
      Function<ValueT, Optional<ResultT>> extractor,
      ProcessingContext context) {
    return Optional.ofNullable(directive.getArgument(argumentName))
        .map(Argument::getValue)
        .filter(
            v -> {
              if (valueClass.isInstance(v)) {
                return true;
              } else {
                context.addError(
                    directive.getSourceLocation(),
                    ProcessingErrorType.InvalidSyntax,
                    "%s.%s: invalid value, expected a %s",
                    directive.getName(),
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
