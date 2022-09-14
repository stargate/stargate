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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import graphql.Scalars;
import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.idl.TypeUtil;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IncrementModelBuilder extends ArgumentDirectiveModelBuilderBase<IncrementModel> {

  // The types that we allow for an argument that represents an increment to a counter:
  private static final List<String> COUNTER_INCREMENT_TYPES =
      ImmutableList.of(
          Scalars.GraphQLInt.getName(),
          CqlScalar.BIGINT.getGraphqlType().getName(),
          CqlScalar.COUNTER.getGraphqlType().getName());
  private static final boolean PREPEND_DEFAULT = false;

  public IncrementModelBuilder(
      InputValueDefinition argument,
      Optional<Directive> directive,
      EntityModel entity,
      FieldModel field,
      String operationName,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    super(argument, directive, entity, field, operationName, entities, context);
  }

  @Override
  IncrementModel build() throws SkipException {
    boolean prepend =
        directive
            .flatMap(d -> DirectiveHelper.getBooleanArgument(d, "prepend", context))
            .orElse(PREPEND_DEFAULT);

    validate(field, prepend);
    return new IncrementModel(field, prepend, argument.getName());
  }

  protected void validate(FieldModel field, boolean prepend) throws SkipException {
    if (field.isPartitionKey() || field.isClusteringColumn()) {
      invalidMapping(
          "Operation %s: directive %s is not supported for partition/clustering key field %s.",
          operationName, CqlDirectives.INCREMENT, field.getGraphqlName());
      throw SkipException.INSTANCE;
    }

    Type<?> fieldInputType = fieldInputType();

    if (isCounter(fieldInputType)) {
      if (prepend) {
        failOnInvalidPrepend();
      }
      if (!argumentIsValidCounterIncrementType()) {
        invalidMapping(
            "Operation %s: expected argument %s to have a valid counter increment type (one of: %s)",
            operationName, argument.getName(), Joiner.on(", ").join(COUNTER_INCREMENT_TYPES));
        throw SkipException.INSTANCE;
      }
    } else if (TypeUtil.isList(fieldInputType)) {
      if (field.getCqlType().hasSet() && prepend) {
        failOnInvalidPrepend();
      }
      Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
      if (!TypeHelper.deepEquals(argumentType, fieldInputType)) {
        invalidMapping(
            "Operation %s: expected argument %s to have type: %s to match %s.%s",
            operationName,
            argument.getName(),
            TypeHelper.format(fieldInputType),
            entity.getGraphqlName(),
            field.getGraphqlName());
        throw SkipException.INSTANCE;
      }
    } else {
      invalidMapping(
          "Operation %s: @%s can only be applied to counter or collection fields",
          operationName, CqlDirectives.INCREMENT);
      throw SkipException.INSTANCE;
    }
  }

  private boolean isCounter(Type<?> type) {
    return type instanceof TypeName
        && CqlScalar.COUNTER.getGraphqlType().getName().equals(((TypeName) type).getName());
  }

  private void failOnInvalidPrepend() throws SkipException {
    invalidMapping(
        "Operation %s: @%s.%s can only be applied to list fields",
        operationName, CqlDirectives.INCREMENT, CqlDirectives.INCREMENT_PREPEND);
    throw SkipException.INSTANCE;
  }

  private boolean argumentIsValidCounterIncrementType() {
    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());
    if (argumentType instanceof TypeName) {
      String argumentTypeName = ((TypeName) argumentType).getName();
      for (String allowedTypeName : COUNTER_INCREMENT_TYPES) {
        if (argumentTypeName.equals(allowedTypeName)) {
          return true;
        }
      }
    }
    return false;
  }
}
