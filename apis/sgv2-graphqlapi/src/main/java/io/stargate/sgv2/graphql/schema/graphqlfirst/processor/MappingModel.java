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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.GraphqlErrorException;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** How a custom, user-submitted GraphQL schema will be mapped to CQL. */
public class MappingModel {

  private static final Logger LOG = LoggerFactory.getLogger(MappingModel.class);
  private static final Predicate<ObjectTypeDefinition> IS_RESPONSE_PAYLOAD =
      t -> DirectiveHelper.getDirective(CqlDirectives.PAYLOAD, t).isPresent();

  private final Map<String, EntityModel> entities;
  private final Map<String, ResponsePayloadModel> responses;
  private final List<OperationModel> operations;
  private final boolean hasUserQueries;

  MappingModel(
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responses,
      List<OperationModel> operations,
      boolean hasUserQueries) {
    this.entities = entities;
    this.responses = responses;
    this.operations = operations;
    this.hasUserQueries = hasUserQueries;
  }

  public Map<String, EntityModel> getEntities() {
    return entities;
  }

  public Map<String, ResponsePayloadModel> getResponses() {
    return responses;
  }

  public boolean hasFederatedEntities() {
    return getEntities().values().stream().anyMatch(EntityModel::isFederated);
  }

  public List<OperationModel> getOperations() {
    return operations;
  }

  public boolean hasUserQueries() {
    return hasUserQueries;
  }

  /** @throws GraphqlErrorException if the model contains mapping errors */
  static MappingModel build(TypeDefinitionRegistry registry, ProcessingContext context) {

    Optional<ObjectTypeDefinition> maybeQueryType = getOperationType(registry, "query", "Query");
    Optional<ObjectTypeDefinition> maybeMutationType =
        getOperationType(registry, "mutation", "Mutation");
    Optional<ObjectTypeDefinition> maybeSubscriptionType =
        getOperationType(registry, "subscription", "Subscription");
    maybeSubscriptionType.ifPresent(
        t ->
            context.addError(
                t.getSourceLocation(),
                ProcessingErrorType.InvalidMapping,
                "This GraphQL implementation does not support subscriptions"));

    // Don't map the default Query, Mutation and Subscription types to CQL tables:
    Set<ObjectTypeDefinition> typesToIgnore =
        ImmutableList.of(maybeQueryType, maybeMutationType, maybeSubscriptionType).stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

    Map<String, EntityModel> entities = buildEntities(registry, typesToIgnore, context);
    // Query is required, but if there are federated entities an `_entities` query will
    // automatically be added later, so we don't require any user-defined queries.
    if (!maybeQueryType.isPresent()
        && entities.values().stream().noneMatch(EntityModel::isFederated)) {
      context.addError(
          null,
          ProcessingErrorType.InvalidSyntax,
          "A schema MUST have a 'query' operation defined");
    }

    Map<String, ResponsePayloadModel> responsePayloads =
        buildResponsePayloads(registry, typesToIgnore, entities, context);

    ImmutableList.Builder<OperationModel> operationsBuilder = ImmutableList.builder();
    maybeQueryType.ifPresent(
        queryType ->
            buildQueries(queryType, entities, responsePayloads, operationsBuilder, context));
    maybeMutationType.ifPresent(
        mutationType ->
            buildMutations(mutationType, entities, responsePayloads, operationsBuilder, context));

    if (!context.getErrors().isEmpty()) {
      // No point in continuing to validation if the model is broken
      String schemaOrigin =
          context.isPersisted() ? "stored for this keyspace" : "that you provided";
      throw GraphqlErrorException.newErrorException()
          .message(
              String.format(
                  "The GraphQL schema %s contains CQL mapping errors. See details in `extensions.mappingErrors` below.",
                  schemaOrigin))
          .extensions(ImmutableMap.of("mappingErrors", context.getErrors()))
          .build();
    }
    return new MappingModel(
        entities, responsePayloads, operationsBuilder.build(), maybeQueryType.isPresent());
  }

  /**
   * Finds the GraphQL default operation container types: Query, Mutation and Subscription.
   *
   * <p>They can be declared either directly with their default name:
   *
   * <pre>
   * type Query { ... }
   * </pre>
   *
   * Or on the schema element with a custom name:
   *
   * <pre>
   * schema { query: MyCustomQueryType }
   * type MyCustomQueryType { ... }
   * </pre>
   */
  private static Optional<ObjectTypeDefinition> getOperationType(
      TypeDefinitionRegistry registry, String fieldName, String defaultTypeName) {
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

  /**
   * Analyze each GraphQL output type, and try to map it as an "entity" that will have a
   * corresponding CQL table or UDT.
   */
  private static Map<String, EntityModel> buildEntities(
      TypeDefinitionRegistry registry,
      Set<ObjectTypeDefinition> typesToIgnore,
      ProcessingContext context) {
    ImmutableMap.Builder<String, EntityModel> entitiesBuilder = ImmutableMap.builder();
    registry.getTypes(ObjectTypeDefinition.class).stream()
        .filter(t -> !typesToIgnore.contains(t))
        .filter(IS_RESPONSE_PAYLOAD.negate())
        .forEach(
            type -> {
              try {
                entitiesBuilder.put(type.getName(), new EntityModelBuilder(type, context).build());
              } catch (SkipException e) {
                LOG.debug(
                    "Skipping type {} because it has mapping errors, "
                        + "this will be reported after the whole schema has been processed.",
                    type.getName());
              }
            });
    return entitiesBuilder.build();
  }

  /**
   * Analyze each GraphQL output type, and try to map it as a "payload": this is a transient type
   * that is only used as a result of an operation. It is not mapped to a CQL table.
   */
  private static Map<String, ResponsePayloadModel> buildResponsePayloads(
      TypeDefinitionRegistry registry,
      Set<ObjectTypeDefinition> typesToIgnore,
      Map<String, EntityModel> entities,
      ProcessingContext context) {
    ImmutableMap.Builder<String, ResponsePayloadModel> responsePayloadsBuilder =
        ImmutableMap.builder();
    registry.getTypes(ObjectTypeDefinition.class).stream()
        .filter(t -> !typesToIgnore.contains(t))
        .filter(IS_RESPONSE_PAYLOAD)
        .forEach(
            type -> {
              responsePayloadsBuilder.put(
                  type.getName(), new ResponsePayloadModelBuilder(type, entities, context).build());
            });
    return responsePayloadsBuilder.build();
  }

  /** Analyze each GraphQL query and try to generate a CQL query for it. */
  private static void buildQueries(
      ObjectTypeDefinition queryType,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ImmutableList.Builder<OperationModel> operationsBuilder,
      ProcessingContext context) {
    for (FieldDefinition query : queryType.getFieldDefinitions()) {
      try {
        operationsBuilder.add(
            new QueryModelBuilder(query, queryType.getName(), entities, responsePayloads, context)
                .build());
      } catch (SkipException e) {
        LOG.debug(
            "Skipping query {} because it has mapping errors, "
                + "this will be reported after the whole schema has been processed.",
            query.getName());
      }
    }
  }

  /** Analyze each GraphQL mutation and try to generate a CQL query for it. */
  private static void buildMutations(
      ObjectTypeDefinition mutationType,
      Map<String, EntityModel> entities,
      Map<String, ResponsePayloadModel> responsePayloads,
      ImmutableList.Builder<OperationModel> operationsBuilder,
      ProcessingContext context) {
    for (FieldDefinition mutation : mutationType.getFieldDefinitions()) {
      try {
        operationsBuilder.add(
            MutationModelFactory.build(
                mutation, mutationType.getName(), entities, responsePayloads, context));
      } catch (SkipException e) {
        LOG.debug(
            "Skipping mutation {} because it has mapping errors, "
                + "this will be reported after the whole schema has been processed.",
            mutation.getName());
      }
    }
  }
}
