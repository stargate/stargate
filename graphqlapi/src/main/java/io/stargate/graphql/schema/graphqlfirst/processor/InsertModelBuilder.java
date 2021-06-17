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
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import java.util.Arrays;
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

    // Validate return type: must be the entity itself, or a wrapper payload
    ReturnType returnType = getReturnType("Mutation " + operationName);
    if (returnType.isEntityList()
        || !returnType.getEntity().filter(e -> e.equals(entity)).isPresent()) {
      invalidMapping(
          "Mutation %s: invalid return type. Expected %s, or a response payload that wraps a "
              + "single instance of it.",
          operationName, entity.getGraphqlName());
    }

    // we are using GraphQLString because it will be coerced to BigInteger (Long)
    Optional<String> cqlTimestampArgumentName =
        findFieldNameWithDirective(
            CqlDirectives.TIMESTAMP, Arrays.asList(Scalars.GraphQLString, Scalars.GraphQLInt));

    Optional<ResponsePayloadModel> responsePayload =
        Optional.of(returnType)
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
        cqlTimestampArgumentName);
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
