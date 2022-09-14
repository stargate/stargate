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

import graphql.language.Directive;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ArgumentDirectiveModelsBuilder.OperationType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ResponsePayloadModel.TechnicalField;
import java.util.Collections;
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
    Optional<Directive> cqlDeleteDirective =
        DirectiveHelper.getDirective(CqlDirectives.DELETE, operation);
    boolean ifExists = computeIfExists(cqlDeleteDirective);

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
      if (!unsupportedFields.isEmpty()) {
        warn(
            "Mutation %s: 'applied' is the only supported field in delete response payloads. Others will always be null (%s).",
            operationName, String.join(", ", unsupportedFields));
      }
    }

    List<InputValueDefinition> arguments = operation.getInputValueDefinitions();
    if (arguments.isEmpty()) {
      invalidMapping(
          "Mutation %s: deletes must take either the entity input type or a list of primary key fields",
          operationName);
      throw SkipException.INSTANCE;
    }

    EntityModel entity;
    InputValueDefinition firstArgument = arguments.get(0);
    Optional<EntityModel> entityFromFirstArgument = findEntity(firstArgument);
    Optional<String> entityArgumentName =
        entityFromFirstArgument.map(__ -> firstArgument.getName());
    List<ConditionModel> whereConditions;
    List<ConditionModel> ifConditions;
    if (entityFromFirstArgument.isPresent()) {
      if (arguments.size() > 1) {
        invalidMapping(
            "Mutation %s: if a delete takes an entity input type, it must be the only argument",
            operationName);
        throw SkipException.INSTANCE;
      }
      entity = entityFromFirstArgument.get();
      whereConditions = entity.getPrimaryKeyWhereConditions();
      ifConditions = Collections.emptyList();
    } else {
      entity = entityFromDirective(cqlDeleteDirective, "delete", CqlDirectives.DELETE);
      ArgumentDirectiveModels conditions =
          new ArgumentDirectiveModelsBuilder(
                  operation, OperationType.DELETE, entity, entities, context)
              .build();
      whereConditions = conditions.getWhereConditions();
      ifConditions = conditions.getIfConditions();
      validateNoFiltering(whereConditions, entity);
      if (!ifConditions.isEmpty() && ifExists) {
        invalidMapping(
            "Operation %s: can't use @%s and %s at the same time",
            operationName, CqlDirectives.IF, CqlDirectives.UPDATE_OR_DELETE_IF_EXISTS);
        throw SkipException.INSTANCE;
      }
    }

    return new DeleteModel(
        parentTypeName,
        operation,
        entity,
        entityArgumentName,
        whereConditions,
        ifConditions,
        returnType,
        ifExists,
        getConsistencyLevel(cqlDeleteDirective),
        getSerialConsistencyLevel(cqlDeleteDirective),
        context.getKeyspace());
  }
}
