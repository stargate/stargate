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
import graphql.language.ListType;
import graphql.language.Type;
import graphql.language.TypeName;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

abstract class MutationModelBuilder extends OperationModelBuilderBase<MutationModel> {

  protected MutationModelBuilder(
      FieldDefinition operation,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ProcessingContext context) {
    super(operation, entities, responsePayloads, context);
  }

  protected Optional<EntityModel> findEntity(InputValueDefinition input) {

    Type<?> type = TypeHelper.unwrapNonNull(input.getType());

    String inputTypeName;
    if (type instanceof ListType) {
      Type<?> innerListType = TypeHelper.unwrapNonNull(((ListType) type).getType());
      inputTypeName = ((TypeName) innerListType).getName();

    } else {
      inputTypeName = ((TypeName) type).getName();
    }

    return entities.values().stream()
        .filter(e -> e.getInputTypeName().map(name -> name.equals(inputTypeName)).orElse(false))
        .findFirst();
  }

  protected boolean isList(InputValueDefinition input) {
    Type<?> type = TypeHelper.unwrapNonNull(input.getType());
    return (type instanceof ListType);
  }

  protected EntityModel entityFromDirective(
      Optional<Directive> cqlDeleteDirective, String name, String directive) throws SkipException {
    EntityModel entity;
    String entityName =
        cqlDeleteDirective
            .flatMap(
                d ->
                    DirectiveHelper.getStringArgument(
                        d, CqlDirectives.UPDATE_OR_DELETE_TARGET_ENTITY, context))
            .orElseThrow(
                () -> {
                  invalidMapping(
                      "Mutation %s: if a %s doesn't take an entity input type, "
                          + "it must indicate the entity name in '@%s.%s'",
                      operationName, name, directive, CqlDirectives.UPDATE_OR_DELETE_TARGET_ENTITY);
                  return SkipException.INSTANCE;
                });
    entity = entities.get(entityName);
    if (entity == null) {
      invalidMapping(
          "Mutation %s: unknown entity %s (from '@%s.%s')",
          operationName, entityName, directive, CqlDirectives.UPDATE_OR_DELETE_TARGET_ENTITY);
      throw SkipException.INSTANCE;
    }
    return entity;
  }

  protected boolean computeIfExists(Optional<Directive> directive) {
    return directive
        .flatMap(
            d ->
                DirectiveHelper.getBooleanArgument(
                    d, CqlDirectives.UPDATE_OR_DELETE_IF_EXISTS, context))
        .orElseGet(
            () -> {
              if (operation.getName().endsWith("IfExists")) {
                info(
                    "Mutation %s: setting the '%s' flag implicitly "
                        + "because the name follows the naming convention.",
                    operationName, CqlDirectives.UPDATE_OR_DELETE_IF_EXISTS);
                return true;
              }
              return false;
            });
  }

  protected Optional<ConsistencyLevel> getConsistencyLevel(Optional<Directive> directive) {
    return directive.flatMap(
        d ->
            DirectiveHelper.getEnumArgument(
                d, CqlDirectives.MUTATION_CONSISTENCY_LEVEL, ConsistencyLevel.class, context));
  }

  protected Optional<ConsistencyLevel> getSerialConsistencyLevel(Optional<Directive> directive) {
    return directive.flatMap(
        d ->
            DirectiveHelper.getEnumArgument(
                d,
                CqlDirectives.MUTATION_SERIAL_CONSISTENCY_LEVEL,
                ConsistencyLevel.class,
                context));
  }
}
