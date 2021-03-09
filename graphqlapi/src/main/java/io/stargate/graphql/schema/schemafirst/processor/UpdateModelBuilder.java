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
import java.util.List;
import java.util.Map;

class UpdateModelBuilder extends MutationModelBuilder {

  private final String parentTypeName;

  UpdateModelBuilder(
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

    // TODO more options for signature
    // Currently requiring exactly one argument that must be an entity input with all PK fields set.
    // We could also take the PK fields directly (need a way to specify the entity), partial PKs for
    // multi-row deletions, additional IF conditions, etc.

    ReturnType returnType = getReturnType("Mutation " + operationName);
    if (returnType != SimpleReturnType.BOOLEAN) {
      invalidMapping("Mutation %s: updates can only return Boolean", operationName);
      throw SkipException.INSTANCE;
    }

    List<InputValueDefinition> inputs = operation.getInputValueDefinitions();
    if (inputs.isEmpty()) {
      invalidMapping(
          "Mutation %s: updates must take the entity input type as the first argument",
          operationName);
      throw SkipException.INSTANCE;
    }

    if (inputs.size() > 1) {
      invalidMapping("Mutation %s: updates can't have more than one argument", operationName);
      throw SkipException.INSTANCE;
    }

    InputValueDefinition input = inputs.get(0);
    EntityModel entity =
        findEntity(input)
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Mutation %s: unexpected argument type, "
                          + "updates expect an input object that maps to a CQL entity",
                      operationName);
                  return SkipException.INSTANCE;
                });
    return new UpdateModel(parentTypeName, operation, entity, input.getName());
  }
}
