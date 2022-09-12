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

import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLScalarType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.EntityListReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.EntityReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.ResponsePayloadModelListReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.SimpleListReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

abstract class OperationModelBuilderBase<T extends OperationModel> extends ModelBuilderBase<T> {

  protected final FieldDefinition operation;
  protected final String operationName;
  protected final Map<String, EntityModel> entities;
  protected final Map<String, ResponsePayloadModel> responsePayloads;

  protected OperationModelBuilderBase(
      FieldDefinition operation,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(context, operation.getSourceLocation());
    this.operation = operation;
    this.operationName = operation.getName();
    this.entities = entities;
    this.responsePayloads = responsePayloads;
  }

  OperationModel.ReturnType getReturnType(String operationDescription) throws SkipException {
    Type<?> graphqlType = TypeHelper.unwrapNonNull(operation.getType());

    if (graphqlType instanceof ListType) {
      OperationModel.ReturnType listReturnType = toListReturnType((ListType) graphqlType);
      if (listReturnType != null) return listReturnType;
    } else {
      assert graphqlType instanceof TypeName;
      String typeName = ((TypeName) graphqlType).getName();

      SimpleReturnType simple = SimpleReturnType.fromTypeName(typeName);
      if (simple != null) {
        return simple;
      }

      EntityModel entity = entities.get(typeName);
      if (entity != null) {
        return new EntityReturnType(entity);
      }

      ResponsePayloadModel payload = responsePayloads.get(typeName);
      if (payload != null) {
        return payload;
      }
    }
    invalidMapping(
        "%s: unsupported return type %s", operationDescription, TypeHelper.format(graphqlType));
    throw SkipException.INSTANCE;
  }

  private OperationModel.ReturnType toListReturnType(ListType graphqlType) {
    Type<?> elementType = graphqlType.getType();
    elementType = TypeHelper.unwrapNonNull(elementType);
    if (elementType instanceof TypeName) {
      String typeName = ((TypeName) elementType).getName();

      // handle entity type
      EntityModel entity = entities.get(typeName);
      if (entity != null) {
        return new EntityListReturnType(entity);
      }

      // handle response payload
      ResponsePayloadModel payload = responsePayloads.get(typeName);
      if (payload != null) {
        return new ResponsePayloadModelListReturnType(payload);
      }

      // handle simple type
      SimpleReturnType simple = SimpleReturnType.fromTypeName(typeName);
      if (simple != null) {
        return new SimpleListReturnType(simple);
      }
    }
    return null;
  }

  protected void validateNoFiltering(List<ConditionModel> whereConditions, EntityModel entity)
      throws SkipException {
    Optional<String> maybeError = entity.validateNoFiltering(whereConditions);
    if (maybeError.isPresent()) {
      invalidMapping("Operation %s: %s", operationName, maybeError.get());
      throw SkipException.INSTANCE;
    }
  }

  protected Optional<String> findFieldNameWithDirective(
      String directiveName, GraphQLScalarType... expectedTypes) throws SkipException {
    Optional<String> result = Optional.empty();
    for (InputValueDefinition inputValue : operation.getInputValueDefinitions()) {
      if (hasDirectiveAndType(inputValue, directiveName, expectedTypes)) {
        if (result.isPresent()) {
          invalidMapping(
              "Query %s: @%s can be used on at most one argument (found %s and %s)",
              operationName, directiveName, result.get(), inputValue.getName());
          throw SkipException.INSTANCE;
        }
        result = Optional.of(inputValue.getName());
      }
    }
    return result;
  }

  private boolean hasDirectiveAndType(
      InputValueDefinition inputValue, String directiveName, GraphQLScalarType... expectedTypes)
      throws SkipException {
    boolean hasDirective = DirectiveHelper.getDirective(directiveName, inputValue).isPresent();
    if (!hasDirective) {
      return false;
    }
    Type<?> type = TypeHelper.unwrapNonNull(inputValue.getType());
    for (GraphQLScalarType expectedType : expectedTypes) {
      String name = expectedType.getName();
      if (type instanceof TypeName && ((TypeName) type).getName().equals(name)) {
        CqlScalar.fromGraphqlName(name).ifPresent(context.getUsedCqlScalars()::add);
        return true;
      }
    }
    handleTypeMismatch(inputValue, directiveName, expectedTypes);
    return false;
  }

  private void handleTypeMismatch(
      InputValueDefinition inputValue, String directiveName, GraphQLScalarType... expectedTypes)
      throws SkipException {
    if (expectedTypes.length == 1) {
      invalidMapping(
          "Query %s: argument %s annotated with @%s must have type %s",
          operationName, inputValue.getName(), directiveName, expectedTypes[0].getName());
    } else {
      invalidMapping(
          "Query %s: argument %s annotated with @%s must have one of the types %s",
          operationName, inputValue.getName(), directiveName, mapToNames(expectedTypes));
    }
    throw SkipException.INSTANCE;
  }

  private List<String> mapToNames(GraphQLScalarType... expectedTypes) {
    return Arrays.stream(expectedTypes)
        .map(GraphQLScalarType::getName)
        .collect(Collectors.toList());
  }
}
