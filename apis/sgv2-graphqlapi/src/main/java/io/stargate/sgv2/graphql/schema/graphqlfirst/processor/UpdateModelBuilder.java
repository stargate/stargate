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
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ArgumentDirectiveModelsBuilder.OperationType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.ReturnType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.OperationModel.SimpleReturnType;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    Optional<Directive> cqlUpdateDirective =
        DirectiveHelper.getDirective(CqlDirectives.UPDATE, operation);
    boolean ifExists = computeIfExists(cqlUpdateDirective);

    ReturnType returnType = getReturnType("Mutation " + operationName);
    if (returnType != SimpleReturnType.BOOLEAN && !(returnType instanceof ResponsePayloadModel)) {
      invalidMapping(
          "Mutation %s: invalid return type. Expected Boolean or a response payload",
          operationName);
      throw SkipException.INSTANCE;
    }

    ResponsePayloadModel payload = null;
    if (returnType instanceof ResponsePayloadModel) {
      payload = (ResponsePayloadModel) returnType;
      Set<String> unsupportedFields =
          payload.getTechnicalFields().stream()
              .filter(f -> f != ResponsePayloadModel.TechnicalField.APPLIED)
              .map(ResponsePayloadModel.TechnicalField::getGraphqlName)
              .collect(Collectors.toCollection(HashSet::new));
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
    List<ConditionModel> whereConditions;
    List<ConditionModel> ifConditions;
    List<IncrementModel> incrementModels;

    if (entityFromFirstArgument.isPresent()) {
      if (arguments.size() > 1) {
        invalidMapping(
            "Mutation %s: if an update takes an entity input type, it must be the only argument",
            operationName);
        throw SkipException.INSTANCE;
      }
      entity = entityFromFirstArgument.get();
      whereConditions = entity.getPrimaryKeyWhereConditions();
      ifConditions = Collections.emptyList();
      incrementModels = Collections.emptyList();
    } else {
      entity = entityFromDirective(cqlUpdateDirective, "update", CqlDirectives.UPDATE);
      ArgumentDirectiveModels directives =
          new ArgumentDirectiveModelsBuilder(
                  operation, OperationType.UPDATE, entity, entities, context)
              .build();
      whereConditions = directives.getWhereConditions();
      ifConditions = directives.getIfConditions();
      validate(whereConditions, ifConditions, ifExists, entity);
      incrementModels = directives.getIncrementModels();
    }

    Optional<String> cqlTimestampArgumentName =
        findFieldNameWithDirective(
            CqlDirectives.TIMESTAMP, Scalars.GraphQLString, CqlScalar.BIGINT.getGraphqlType());

    return new UpdateModel(
        parentTypeName,
        operation,
        entity,
        whereConditions,
        ifConditions,
        entityArgumentName,
        returnType,
        Optional.ofNullable(payload),
        ifExists,
        incrementModels,
        getConsistencyLevel(cqlUpdateDirective),
        getSerialConsistencyLevel(cqlUpdateDirective),
        getTtl(cqlUpdateDirective),
        cqlTimestampArgumentName,
        context.getKeyspace());
  }

  private void validate(
      List<ConditionModel> whereConditions,
      List<ConditionModel> ifConditions,
      boolean ifExists,
      EntityModel entity)
      throws SkipException {

    Optional<String> maybeError = entity.validateForUpdate(whereConditions);
    if (maybeError.isPresent()) {
      invalidMapping("Operation %s: %s", operationName, maybeError.get());
      throw SkipException.INSTANCE;
    }

    if (!ifConditions.isEmpty()) {
      ensureNoInConditions(whereConditions);
      if (ifExists) {
        invalidMapping(
            "Operation %s: can't use @%s and %s at the same time",
            operationName, CqlDirectives.IF, CqlDirectives.UPDATE_OR_DELETE_IF_EXISTS);
        throw SkipException.INSTANCE;
      }
    }
  }

  private void ensureNoInConditions(List<ConditionModel> whereConditions) throws SkipException {
    for (ConditionModel whereCondition : whereConditions) {
      if (whereCondition.getPredicate() == Predicate.IN) {
        invalidMapping(
            "Operation %s: IN predicates on primary key fields are not allowed "
                + "if there are @%s conditions (%s)",
            operationName, CqlDirectives.IF, whereCondition.getArgumentName());
        throw SkipException.INSTANCE;
      }
    }
  }
}
