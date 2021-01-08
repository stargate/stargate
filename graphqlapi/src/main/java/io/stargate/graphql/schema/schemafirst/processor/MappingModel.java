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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.GraphqlErrorException;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MappingModel {

  private final Map<String, EntityMappingModel> entities;
  private final List<QueryMappingModel> queries;

  MappingModel(Map<String, EntityMappingModel> entities, List<QueryMappingModel> queries) {
    this.entities = entities;
    this.queries = queries;
  }

  Map<String, EntityMappingModel> getEntities() {
    return entities;
  }

  List<QueryMappingModel> getQueries() {
    return queries;
  }

  /** @throws GraphqlErrorException */
  static MappingModel build(TypeDefinitionRegistry registry, ProcessingContext context) {

    ImmutableMap.Builder<String, EntityMappingModel> entitiesBuilder = ImmutableMap.builder();

    // The Query type is always present (otherwise the GraphQL parser would have failed)
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    ObjectTypeDefinition queryType = getOperationType(registry, "query", "Query").get();
    Optional<ObjectTypeDefinition> maybeMutationType =
        getOperationType(registry, "mutation", "Mutation");
    Optional<ObjectTypeDefinition> maybeSubscriptionType =
        getOperationType(registry, "subscription", "Subscription");

    Set<ObjectTypeDefinition> typesToIgnore = new HashSet<>();
    typesToIgnore.add(queryType);
    maybeMutationType.ifPresent(typesToIgnore::add);
    maybeSubscriptionType.ifPresent(
        t -> {
          context.addError(
              t.getSourceLocation(),
              ProcessingMessageType.InvalidMapping,
              "This GraphQL implementation does not support subscriptions");
          typesToIgnore.add(t);
        });

    for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
      if (typesToIgnore.contains(type)) {
        continue;
      }
      EntityMappingModel.build(type, context)
          .ifPresent(entity -> entitiesBuilder.put(type.getName(), entity));
    }
    Map<String, EntityMappingModel> entities = entitiesBuilder.build();

    ImmutableList.Builder<QueryMappingModel> queriesBuilder = ImmutableList.builder();
    for (FieldDefinition query : queryType.getFieldDefinitions()) {
      QueryMappingModel.build(query, queryType.getName(), entities, context)
          .ifPresent(queriesBuilder::add);
    }

    if (!context.getErrors().isEmpty()) {
      // No point in continuing to validation if the model is broken
      throw GraphqlErrorException.newErrorException()
          .message(
              "The schema you provided contains CQL mapping errors. "
                  + "See details in `extensions.mappingErrors` below.")
          .extensions(ImmutableMap.of("mappingErrors", context.getErrors()))
          .build();
    }
    return new MappingModel(entities, queriesBuilder.build());
  }

  private static Optional<ObjectTypeDefinition> getOperationType(
      TypeDefinitionRegistry registry, String fieldName, String defaultTypeName) {

    // Operation types can have custom names if the schema is explicitly declared, e.g:
    //   schema { query: MyQueryRootType }

    String typeName =
        registry
            .schemaDefinition()
            .flatMap(
                schema -> {
                  for (OperationTypeDefinition operation : schema.getOperationTypeDefinitions()) {
                    if (operation.getName().equals(fieldName)) {
                      return Optional.of(operation.getTypeName().getName());
                    }
                  }
                  return Optional.empty();
                })
            .orElse(defaultTypeName);

    return registry
        .getType(typeName)
        .filter(t -> t instanceof ObjectTypeDefinition)
        .map(t -> (ObjectTypeDefinition) t);
  }
}
