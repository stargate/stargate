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

import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.ReturnType;
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

    boolean ifNotExists = computeIfNotExists();

    // Validate inputs: must be a single entity argument
    List<InputValueDefinition> inputs = operation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      invalidMapping(
          "Mutation %s: inserts must take the entity input type as the first argument",
          operationName);
      throw SkipException.INSTANCE;
    }
    if (inputs.size() > 1) {
      invalidMapping("Mutation %s: inserts can't have more than one argument", operationName);
      throw SkipException.INSTANCE;
    }
    InputValueDefinition input = inputs.get(0);
    EntityModel entity = findEntity(input, "insert");

    // Validate return type: must be the entity itself, or a wrapper payload
    ReturnType returnType = getReturnType("Mutation " + operationName);
    if (returnType.isEntityList()
        || !returnType.getEntity().filter(e -> e.equals(entity)).isPresent()) {
      invalidMapping(
          "Mutation %s: invalid return type. Expected %s, or a response payload that wraps a "
              + "single instance of it.",
          operationName, entity.getGraphqlName());
    }

    Optional<ResponsePayloadModel> responsePayload =
        Optional.of(returnType)
            .filter(ResponsePayloadModel.class::isInstance)
            .map(ResponsePayloadModel.class::cast);

    return new InsertModel(
        parentTypeName, operation, entity, input.getName(), responsePayload, ifNotExists);
  }

  private Boolean computeIfNotExists() {
    // If the directive is set, it always takes precedence
    Optional<Boolean> fromDirective =
        DirectiveHelper.getDirective("cql_insert", operation)
            .flatMap(d -> DirectiveHelper.getBooleanArgument(d, "ifNotExists", context));
    if (fromDirective.isPresent()) {
      return fromDirective.get();
    }
    // Otherwise, try the naming convention
    if (operation.getName().endsWith("IfNotExists")) {
      info(
          "Mutation %s: setting the 'ifNotExists' flag implicitly "
              + "because the name follows the naming convention.",
          operationName);
      return true;
    }
    return false;
  }
}
