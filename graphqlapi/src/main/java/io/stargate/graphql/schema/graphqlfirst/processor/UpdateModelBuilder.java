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

import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import io.stargate.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import java.util.*;
import java.util.stream.Collectors;

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
    Optional<Directive> cqlUpdateDirective = DirectiveHelper.getDirective("cql_update", operation);
    boolean ifExists = computeIfExists(cqlUpdateDirective);

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
              .filter(f -> f != ResponsePayloadModel.TechnicalField.APPLIED)
              .map(ResponsePayloadModel.TechnicalField::getGraphqlName)
              .collect(Collectors.toCollection(HashSet::new));
      payload.getEntityField().ifPresent(e -> unsupportedFields.add(e.getName()));
      if (!unsupportedFields.isEmpty()) {
        warn(
            "Mutation %s: 'applied' is the only supported field in update response payloads. Others will always be null (%s).",
            operationName, String.join(", ", unsupportedFields));
      }
    }

    List<InputValueDefinition> arguments = operation.getInputValueDefinitions();
    if (arguments.isEmpty()) {
      invalidMapping(
          "Mutation %s: updates must take either the entity input type or a list of primary key fields",
          operationName);
      throw SkipException.INSTANCE;
    }

    EntityModel entity;
    InputValueDefinition firstArgument = arguments.get(0);
    Optional<EntityModel> entityFromFirstArgument = findEntity(firstArgument);
    Optional<String> entityArgumentName =
        entityFromFirstArgument.map(__ -> firstArgument.getName());
    List<ConditionModel> ifConditions;
    if (entityFromFirstArgument.isPresent()) {
      if (arguments.size() > 1) {
        invalidMapping(
            "Mutation %s: if an update takes an entity input type, it must be the only argument",
            operationName);
        throw SkipException.INSTANCE;
      }
      entity = entityFromFirstArgument.get();
      ifConditions = Collections.emptyList();
    } else {
      entity = entityFromDirective(cqlUpdateDirective, "update", "cql_update");
      ConditionsModelBuilder.Conditions conditions = buildOnlyIfConditions(entity);
      ifConditions = conditions.getIfConditions();
    }

    return new UpdateModel(
        parentTypeName, operation, entity, ifConditions, entityArgumentName, returnType, ifExists);
  }
}
