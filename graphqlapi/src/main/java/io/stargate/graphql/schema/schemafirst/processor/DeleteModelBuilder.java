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
import io.stargate.graphql.schema.schemafirst.processor.OperationModel.SimpleReturnType;
import io.stargate.graphql.schema.schemafirst.processor.ResponsePayloadModel.TechnicalField;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class DeleteModelBuilder extends MutationModelBuilder {

  private final String parentTypeName;

  DeleteModelBuilder(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(mutation, entities, responsePayloads, context);
    this.parentTypeName = parentTypeName;
  }

  @Override
  MutationModel build() throws SkipException {
    // TODO allow partial PK for multi-row deletions

    ReturnType returnType = getReturnType("Mutation " + operationName);
    if (returnType != SimpleReturnType.BOOLEAN && !(returnType instanceof ResponsePayloadModel)) {
      invalidMapping(
          "Mutation %s: invalid return type. Expected Boolean or a response payload",
          operationName);
      throw SkipException.INSTANCE;
    }
    if (returnType instanceof ResponsePayloadModel) {
      ResponsePayloadModel payload = (ResponsePayloadModel) returnType;
      Set<String> unsupportedFields =
          payload.getTechnicalFields().stream()
              .filter(f -> f != TechnicalField.APPLIED)
              .map(TechnicalField::getGraphqlName)
              .collect(Collectors.toCollection(HashSet::new));
      payload.getEntityField().ifPresent(e -> unsupportedFields.add(e.getName()));
      if (!unsupportedFields.isEmpty()) {
        warn(
            "Mutation %s: 'applied' is the only supported field in delete response payloads. Others will always be null (%s).",
            operationName, String.join(", ", unsupportedFields));
      }
    }

    List<InputValueDefinition> inputs = operation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      invalidMapping(
          "Mutation %s: deletes must take the entity input type as the first argument",
          operationName);
      throw SkipException.INSTANCE;
    }

    if (inputs.size() > 1) {
      invalidMapping("Mutation %s: deletes can't have more than one argument", operationName);
      throw SkipException.INSTANCE;
    }

    InputValueDefinition input = inputs.get(0);
    EntityModel entity = findEntity(input, "delete");
    return new DeleteModel(
        parentTypeName, operation, entity, input.getName(), returnType, computeIfExists());
  }

  private boolean computeIfExists() {
    // If the directive is set, it always takes precedence
    Optional<Boolean> fromDirective =
        DirectiveHelper.getDirective("cql_delete", operation)
            .flatMap(d -> DirectiveHelper.getBooleanArgument(d, "ifExists", context));
    if (fromDirective.isPresent()) {
      return fromDirective.get();
    }
    // Otherwise, try the naming convention
    if (operation.getName().endsWith("IfExists")) {
      info(
          "Mutation %s: setting the 'ifExists' flag implicitly "
              + "because the name follows the naming convention.",
          operationName);
      return true;
    }
    return false;
  }
}
