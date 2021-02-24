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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingModel {

  private static final Logger LOG = LoggerFactory.getLogger(MappingModel.class);

  private final Map<String, EntityMappingModel> entities;
  private final List<OperationMappingModel> operations;

  MappingModel(Map<String, EntityMappingModel> entities, List<OperationMappingModel> operations) {
    this.entities = entities;
    this.operations = operations;
  }

  public Map<String, EntityMappingModel> getEntities() {
    return entities;
  }

  public boolean hasFederatedEntities() {
    return getEntities().values().stream().anyMatch(EntityMappingModel::isFederated);
  }

  public List<OperationMappingModel> getOperations() {
    return operations;
  }

  /** @throws GraphqlErrorException if the model contains mapping errors */
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
              ProcessingErrorType.InvalidMapping,
              "This GraphQL implementation does not support subscriptions");
          typesToIgnore.add(t);
        });

    for (ObjectTypeDefinition type : registry.getTypes(ObjectTypeDefinition.class)) {
      if (typesToIgnore.contains(type)) {
        continue;
      }
      try {
        entitiesBuilder.put(type.getName(), new EntityMappingModelBuilder(type, context).build());
      } catch (SkipException e) {
        LOG.debug(
            "Skipping type {} because it has mapping errors, "
                + "this will be reported after the whole schema has been processed.",
            type.getName());
      }
    }
    Map<String, EntityMappingModel> entities = entitiesBuilder.build();

    ImmutableList.Builder<OperationMappingModel> operationsBuilder = ImmutableList.builder();
    for (FieldDefinition query : queryType.getFieldDefinitions()) {
      try {
        operationsBuilder.add(
            QueryMappingModel.build(query, queryType.getName(), entities, context));
      } catch (SkipException e) {
        LOG.debug(
            "Skipping query {} because it has mapping errors, "
                + "this will be reported after the whole schema has been processed.",
            query.getName());
      }
    }
    maybeMutationType.ifPresent(
        mutationType -> {
          for (FieldDefinition mutation : mutationType.getFieldDefinitions()) {
            try {
              operationsBuilder.add(
                  MutationMappingModelFactory.build(
                      mutation, mutationType.getName(), entities, context));
            } catch (SkipException e) {
              LOG.debug(
                  "Skipping mutation {} because it has mapping errors, "
                      + "this will be reported after the whole schema has been processed.",
                  mutation.getName());
            }
          }
        });

    if (!context.getErrors().isEmpty()) {
      // No point in continuing to validation if the model is broken
      String schemaOrigin =
          context.isPersisted() ? "stored for this namespace" : "that you provided";
      throw GraphqlErrorException.newErrorException()
          .message(
              String.format(
                  "The GraphQL schema %s contains CQL mapping errors. See details in `extensions.mappingErrors` below.",
                  schemaOrigin))
          .extensions(ImmutableMap.of("mappingErrors", context.getErrors()))
          .build();
    }
    return new MappingModel(entities, operationsBuilder.build());
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
