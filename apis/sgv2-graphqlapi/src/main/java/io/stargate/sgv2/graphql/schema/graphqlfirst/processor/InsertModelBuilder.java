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

import graphql.Scalars;
import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.ResponsePayloadModelListReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.SimpleListReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class InsertModelBuilder extends MutationModelBuilder {

  private final String parentTypeName;

  InsertModelBuilder(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(mutation, entities, responsePayloads, context);
    this.parentTypeName = parentTypeName;
  }

  @Override
  InsertModel build() throws SkipException {

    Optional<Directive> cqlInsertDirective =
        DirectiveHelper.getDirective(CqlDirectives.INSERT, operation);
    boolean ifNotExists = computeIfNotExists(cqlInsertDirective);

    // Validate inputs: must be a single entity argument
    List<InputValueDefinition> inputs = operation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      invalidMapping(
          "Mutation %s: inserts must take the entity input type as the first argument",
          operationName);
      throw SkipException.INSTANCE;
    }
    if (inputs.size() > 2) {
      invalidMapping(
          "Mutation %s: inserts can't have more than two arguments: entity input and optionally a value with %s directive",
          operationName, CqlDirectives.TIMESTAMP);
      throw SkipException.INSTANCE;
    }
    InputValueDefinition input = inputs.get(0);
    EntityModel entity =
        findEntity(input)
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Mutation %s: unexpected argument type, "
                          + "inserts expect an input object that maps to a CQL entity",
                      operationName);
                  return SkipException.INSTANCE;
                });
    boolean isList = isList(input);

    // Validate return type: must be the entity itself, or a wrapper payload, boolean, or a list of
    // those types
    ReturnType returnType = getReturnType("Mutation " + operationName);
    if (!returnType.getEntity().filter(e -> e.equals(entity)).isPresent()
        && returnType != SimpleReturnType.BOOLEAN
        && !isSimpleListWithBoolean(returnType)) {
      invalidMapping(
          "Mutation %s: invalid return type. Expected %s, or a response payload that wraps a "
              + "single instance of it or Boolean, or a list of those types.",
          operationName, entity.getGraphqlName());
    }

    if (isList && !returnType.isList()) {
      invalidMapping(
          "Mutation %s: invalid return type. For bulk inserts, expected list of %s. ",
          operationName, entity.getGraphqlName());
    }

    Optional<String> cqlTimestampArgumentName =
        findFieldNameWithDirective(
            CqlDirectives.TIMESTAMP, Scalars.GraphQLString, CqlScalar.BIGINT.getGraphqlType());
    if (inputs.size() == 2 && !cqlTimestampArgumentName.isPresent()) {
      invalidMapping(
          "Mutation %s: if you provided two arguments, the second one must be annotated with %s directive.",
          operationName, CqlDirectives.TIMESTAMP);
    }

    Optional<ResponsePayloadModel> responsePayload =
        Optional.of(returnType)
            .map(
                r -> {
                  // if it is a list of response payloads, extract the underlying type
                  if (r instanceof ResponsePayloadModelListReturnType) {
                    return ((ResponsePayloadModelListReturnType) returnType)
                        .getResponsePayloadModel();
                  }
                  return r;
                })
            .filter(ResponsePayloadModel.class::isInstance)
            .map(ResponsePayloadModel.class::cast);

    return new InsertModel(
        parentTypeName,
        operation,
        entity,
        input.getName(),
        responsePayload,
        ifNotExists,
        getConsistencyLevel(cqlInsertDirective),
        getSerialConsistencyLevel(cqlInsertDirective),
        getTtl(cqlInsertDirective),
        returnType,
        cqlTimestampArgumentName,
        isList,
        context.getKeyspace());
  }

  private boolean isSimpleListWithBoolean(ReturnType returnType) {
    return returnType instanceof SimpleListReturnType
        && ((SimpleListReturnType) returnType)
            .getSimpleReturnType()
            .equals(SimpleReturnType.BOOLEAN);
  }

  private boolean computeIfNotExists(Optional<Directive> cqlInsertDirective) {
    // If the directive is set, it always takes precedence
    Optional<Boolean> fromDirective =
        cqlInsertDirective.flatMap(
            d ->
                DirectiveHelper.getBooleanArgument(d, CqlDirectives.INSERT_IF_NOT_EXISTS, context));
    if (fromDirective.isPresent()) {
      return fromDirective.get();
    }
    // Otherwise, try the naming convention
    if (operation.getName().endsWith("IfNotExists")) {
      info(
          "Mutation %s: setting the '%s' flag implicitly "
              + "because the name follows the naming convention.",
          operationName, CqlDirectives.INSERT_IF_NOT_EXISTS);
      return true;
    }
    return false;
  }
}
