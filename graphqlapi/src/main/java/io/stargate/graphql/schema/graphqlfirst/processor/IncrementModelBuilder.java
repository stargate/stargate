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

import graphql.Scalars;
import graphql.language.Directive;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLScalarType;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.graphql.schema.scalars.CqlScalar;
import java.util.*;
import java.util.stream.Collectors;

public class IncrementModelBuilder extends ModelBuilderBase<IncrementModel> {

  public static final String COUNTER_TYPE_NAME = "counter";
  private final EntityModel entity;
  private final FieldModel field;
  private final Map<String, EntityModel> entities;
  private final ProcessingContext context;

  private static final boolean PREPEND_DEFAULT = false;
  private final String operationName;
  private final InputValueDefinition argument;

  public IncrementModelBuilder(
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
    this.context = context;
  }

  @Override
  IncrementModel build() throws SkipException {

    Optional<Directive> directive = DirectiveHelper.getDirective(CqlDirectives.INCREMENT, argument);

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
    } else {
      checkValidForRegularColumn(field, prepend);
    }
  }

  private void checkValidForRegularColumn(FieldModel field, boolean prepend) throws SkipException {

    Type<?> fieldInputType =
        toInput(field.getGraphqlType(), argument, entity, field, entities, operationName);
    // it is non-list type
    if (fieldInputType instanceof TypeName) {
      String typeName = ((TypeName) fieldInputType).getName();
      if (typeName.equalsIgnoreCase(COUNTER_TYPE_NAME)) {
        // counter graph-ql field can be BIGINT or INT
        checkArgumentIsAnyOfTypes(
            Arrays.asList(CqlScalar.BIGINT.getGraphqlType(), Scalars.GraphQLInt));
      }
    }

    if (prepend) {
      checkArgumentIsAList(field);
    }
  }

  private void checkArgumentIsAList(FieldModel field) throws SkipException {
    Type<?> fieldInputType =
        toInput(field.getGraphqlType(), argument, entity, field, entities, operationName);
    if (!(fieldInputType instanceof ListType)) {
      invalidMapping(
          "Operation %s: the %s directive with prepend = true cannot be used with argument %s "
              + "because it is not a list",
          operationName, CqlDirectives.INCREMENT, argument.getName());
      throw SkipException.INSTANCE;
    }
  }

  protected void checkArgumentIsAnyOfTypes(List<GraphQLScalarType> scalarTypes)
      throws SkipException {

    Type<?> argumentType = TypeHelper.unwrapNonNull(argument.getType());

    boolean hasProperType =
        scalarTypes.stream()
            .anyMatch(
                type -> {
                  if (argumentType instanceof TypeName) {
                    return type.getName().equals(((TypeName) argumentType).getName());
                  }
                  return false;
                });

    if (!hasProperType) {
      invalidMapping(
          "Operation %s: expected argument %s to have one of the types: %s to match %s.%s",
          operationName,
          argument.getName(),
          scalarTypes.stream().map(GraphQLScalarType::getName).collect(Collectors.toList()),
          entity.getGraphqlName(),
          field.getGraphqlName());
      throw SkipException.INSTANCE;
    }
  }
}
