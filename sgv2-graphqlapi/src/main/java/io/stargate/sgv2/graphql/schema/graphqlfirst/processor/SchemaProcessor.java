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

import com.apollographql.federation.graphqljava.Federation;
import com.apollographql.federation.graphqljava._FieldSet;
import com.apollographql.federation.graphqljava.tracing.FederatedTracingInstrumentation;
import com.google.common.collect.ImmutableMap;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.GraphqlErrorException;
import graphql.execution.AsyncExecutionStrategy;
import graphql.language.Argument;
import graphql.language.Description;
import graphql.language.Directive;
import graphql.language.DirectiveDefinition;
import graphql.language.DirectiveLocation;
import graphql.language.EnumTypeDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.StringValue;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLSchemaElement;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.schema.SchemaTransformer;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.errors.SchemaProblem;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import graphql.util.TreeTransformerUtil;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.schema.CassandraFetcherExceptionHandler;
import io.stargate.sgv2.graphql.schema.SchemaConstants;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed.FederatedEntity;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed.FederatedEntityFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SchemaProcessor {

  private static final TypeDefinitionRegistry FEDERATION_DIRECTIVES =
      new SchemaParser()
          .parse(
              new InputStreamReader(
                  Objects.requireNonNull(
                      SchemaProcessor.class.getResourceAsStream("/schemafirst/federation.graphql")),
                  StandardCharsets.UTF_8));

  private static final DirectiveDefinition ATOMIC_DIRECTIVE =
      DirectiveDefinition.newDirectiveDefinition()
          .name(SchemaConstants.ATOMIC_DIRECTIVE)
          .description(
              new Description(
                  "Instructs the server to apply the mutations in a LOGGED batch", null, false))
          .directiveLocation(DirectiveLocation.newDirectiveLocation().name("MUTATION").build())
          .build();

  private final StargateBridgeClient bridge;
  private final boolean isPersisted;

  /**
   * @param isPersisted whether we are processing a schema already stored in the database, or a
   *     schema that a user is attempting to deploy. This is just used to customize a couple of
   */
  public SchemaProcessor(StargateBridgeClient bridge, boolean isPersisted) {
    this.bridge = bridge;
    this.isPersisted = isPersisted;
  }

  /**
   * Processes the GraphQL source provided by the user to produce a complete working schema:
   *
   * <ul>
   *   <li>parse the source;
   *   <li>add any missing elements: generated types, built-in directives, etc.
   *   <li>generate the data fetchers
   * </ul>
   *
   * @throws GraphqlErrorException if any error was encountered during processing
   */
  public ProcessedSchema process(String source, CqlKeyspaceDescribe keyspace) {
    TypeDefinitionRegistry registry = parse(source);
    ProcessingContext context = new ProcessingContext(registry, keyspace, bridge, isPersisted);
    MappingModel mappingModel = MappingModel.build(registry, context);

    addGeneratedTypes(mappingModel, registry);

    GraphQL graphql = buildGraphql(registry, mappingModel, context.getUsedCqlScalars(), keyspace);

    return new ProcessedSchema(mappingModel, graphql, context.getLogs());
  }

  private void addGeneratedTypes(MappingModel mappingModel, TypeDefinitionRegistry registry) {
    mappingModel.getEntities().values().stream()
        .filter(e -> e.getInputTypeName().isPresent())
        // TODO maybe allow the input type to already exist
        // The annotation would only reference it. This would allow users to write their own if for
        // some reason the generated version doesn't work.
        .forEach(e -> registry.add(generateInputType(e, mappingModel)));
  }

  private InputObjectTypeDefinition generateInputType(
      EntityModel entity, MappingModel mappingModel) {
    assert entity.getInputTypeName().isPresent();
    InputObjectTypeDefinition.Builder builder =
        InputObjectTypeDefinition.newInputObjectDefinition().name(entity.getInputTypeName().get());
    entity
        .getAllColumns()
        .forEach(
            c -> {
              Type<?> type = c.getGraphqlType();
              // Our INSERT implementation generates missing PK fields if their type is ID, so make
              // them nullable in the input type.
              if (c.isPrimaryKey() && TypeHelper.mapsToUuid(type) && type instanceof NonNullType) {
                type = ((NonNullType) type).getType();
              }
              type = substituteUdtInputTypes(type, mappingModel);
              builder.inputValueDefinition(
                  InputValueDefinition.newInputValueDefinition()
                      .name(c.getGraphqlName())
                      .type(type)
                      .build());
            });
    return builder.build();
  }

  private Type<?> substituteUdtInputTypes(Type<?> type, MappingModel mappingModel) {
    if (type instanceof ListType) {
      Type<?> elementType = ((ListType) type).getType();
      return ListType.newListType(substituteUdtInputTypes(elementType, mappingModel)).build();
    }
    if (type instanceof NonNullType) {
      Type<?> elementType = ((NonNullType) type).getType();
      return NonNullType.newNonNullType(substituteUdtInputTypes(elementType, mappingModel)).build();
    }
    assert type instanceof TypeName;
    EntityModel entity = mappingModel.getEntities().get(((TypeName) type).getName());
    if (entity != null) {
      // We've already checked this while building the mapping model
      assert entity.getInputTypeName().isPresent();
      return TypeName.newTypeName(entity.getInputTypeName().get()).build();
    }
    return type;
  }

  private GraphQL buildGraphql(
      TypeDefinitionRegistry registry,
      MappingModel mappingModel,
      EnumSet<CqlScalar> cqlScalars,
      CqlKeyspaceDescribe keyspace) {

    // SchemaGenerator fails if the schema uses directives that are not defined. Add everything we
    // need:
    if (mappingModel.hasFederatedEntities()) {
      registry = registry.merge(FEDERATION_DIRECTIVES); // GraphQL federation directives
      finalizeKeyDirectives(registry, mappingModel);
      stubQueryTypeIfNeeded(registry, mappingModel);
    }
    registry = registry.merge(CqlDirectives.ALL_AS_REGISTRY); // Stargate's own CQL directives

    // Unlike the schema directives, this one is used in queries, and therefore stays at runtime
    registry.add(ATOMIC_DIRECTIVE);

    RuntimeWiring.Builder runtimeWiring =
        RuntimeWiring.newRuntimeWiring()
            .codeRegistry(buildCodeRegistry(mappingModel))
            .scalar(_FieldSet.type);
    for (CqlScalar cqlScalar : cqlScalars) {
      registry.add(
          ScalarTypeDefinition.newScalarTypeDefinition()
              .name(cqlScalar.getGraphqlType().getName())
              .build());
      runtimeWiring.scalar(cqlScalar.getGraphqlType());
    }

    GraphQLSchema schema =
        new SchemaGenerator()
            .makeExecutableSchema(
                SchemaGenerator.Options.defaultOptions(), registry, runtimeWiring.build());

    // However once we have the schema we don't need the CQL directives anymore: they only impact
    // the queries we generate, they're not useful for users of the schema.
    schema = removeCqlDirectives(schema);

    GraphQL.Builder graphqlBuilder;
    if (mappingModel.hasFederatedEntities()) {
      GraphQLSchema federationReadySchema =
          Federation.transform(schema)
              .fetchEntities(new FederatedEntityFetcher(mappingModel, keyspace))
              .resolveEntityType(
                  environment -> {
                    FederatedEntity entity = environment.getObject();
                    return environment.getSchema().getObjectType(entity.getTypeName());
                  })
              .build();
      graphqlBuilder =
          GraphQL.newGraphQL(federationReadySchema)
              .instrumentation(new FederatedTracingInstrumentation());
    } else {
      graphqlBuilder = GraphQL.newGraphQL(schema);
    }
    return graphqlBuilder
        .defaultDataFetcherExceptionHandler(CassandraFetcherExceptionHandler.INSTANCE)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(
            new AsyncExecutionStrategy(CassandraFetcherExceptionHandler.INSTANCE))
        .build();
  }

  /**
   * The federation key is always the CQL primary key, so for convenience we allow users to omit
   * {@code @key.fields}. However it is a required field, so fill it now before we try to validate
   * the schema.
   *
   * @see EntityModelBuilder
   */
  private void finalizeKeyDirectives(TypeDefinitionRegistry registry, MappingModel mappingModel) {
    if (!mappingModel.hasFederatedEntities()) {
      return;
    }
    for (EntityModel entity : mappingModel.getEntities().values()) {
      if (!entity.isFederated()) {
        continue;
      }
      ObjectTypeDefinition type =
          registry
              .getType(entity.getGraphqlName(), ObjectTypeDefinition.class)
              .orElseThrow(
                  () ->
                      new AssertionError(
                          "Should find type in registry since we've built an EntityModel from it"));

      Collection<Directive> keyDirectives = type.getDirectives("key");
      // We only allow one @key directive at the moment.
      assert keyDirectives.size() == 1;
      Directive keyDirective = keyDirectives.iterator().next();
      if (keyDirective.getArgument("fields") != null) {
        // We've already validated it in EntityModelBuilder.
        continue;
      }

      String newFieldsValue =
          entity.getPrimaryKey().stream()
              .map(FieldModel::getGraphqlName)
              .collect(Collectors.joining(" "));
      Directive newKeyDirective =
          Directive.newDirective()
              .name("key")
              .argument(
                  Argument.newArgument("fields", StringValue.newStringValue(newFieldsValue).build())
                      .build())
              .build();
      List<Directive> newDirectives =
          type.getDirectives().stream()
              .map(d -> (d == keyDirective) ? newKeyDirective : d)
              .collect(Collectors.toList());
      ObjectTypeDefinition newType = type.transform(builder -> builder.directives(newDirectives));
      registry.remove(type);
      registry.add(newType);
    }
  }

  /**
   * If there are federated entities, we allow users to omit the Query type, because federation-jvm
   * will generate queries of its own (_entities and _service). However we try to parse the schema
   * before that, so we need a dummy query type.
   */
  private void stubQueryTypeIfNeeded(TypeDefinitionRegistry registry, MappingModel mappingModel) {
    if (!mappingModel.hasUserQueries()) {
      registry.add(
          ObjectTypeDefinition.newObjectTypeDefinition()
              .name("Query")
              .fieldDefinition(
                  // Dummy query. It doesn't need to match the actual signature, federation-jvm will
                  // completely replace it.
                  FieldDefinition.newFieldDefinition()
                      .name("_entities")
                      .type(TypeName.newTypeName("Int").build())
                      .build())
              .build());
    }
  }

  private TypeDefinitionRegistry parse(String source) throws GraphqlErrorException {
    SchemaParser parser = new SchemaParser();
    try {
      return parser.parse(source);
    } catch (SchemaProblem schemaProblem) {
      List<GraphQLError> schemaErrors = schemaProblem.getErrors();
      // Convert to JSON explicitly, because the parser sometimes returns errors that also implement
      // java.lang.Exception (e.g. NonSDLDefinitionError), and those get formatted with a full stack
      // trace by default.
      List<Map<String, Object>> errorsJson =
          schemaErrors.stream().map(GraphQLError::toSpecification).collect(Collectors.toList());

      String schemaOrigin = isPersisted ? "stored for this keyspace" : "that you provided";
      throw GraphqlErrorException.newErrorException()
          .message(
              String.format(
                  "The schema %s is not valid GraphQL. See details in `extensions.schemaErrors` below.",
                  schemaOrigin))
          .extensions(ImmutableMap.of("schemaErrors", errorsJson))
          .build();
    }
  }

  private GraphQLCodeRegistry buildCodeRegistry(MappingModel mappingModel) {
    GraphQLCodeRegistry.Builder builder = GraphQLCodeRegistry.newCodeRegistry();
    for (OperationModel operation : mappingModel.getOperations()) {
      builder.dataFetcher(operation.getCoordinates(), operation.getDataFetcher(mappingModel));
    }
    return builder.build();
  }

  private static GraphQLSchema removeCqlDirectives(GraphQLSchema schema) {
    return new SchemaTransformer()
        .transform(
            schema,
            new GraphQLTypeVisitorStub() {

              @Override
              public TraversalControl visitGraphQLEnumType(
                  GraphQLEnumType node, TraverserContext<GraphQLSchemaElement> context) {
                if (CqlDirectives.ALL_AS_REGISTRY
                    .getType(node.getName())
                    .filter(t -> t instanceof EnumTypeDefinition)
                    .isPresent()) {
                  TreeTransformerUtil.deleteNode(context);
                }
                return TraversalControl.CONTINUE;
              }

              @Override
              public TraversalControl visitGraphQLDirective(
                  GraphQLDirective node, TraverserContext<GraphQLSchemaElement> context) {
                if (CqlDirectives.ALL_AS_REGISTRY
                    .getDirectiveDefinition(node.getName())
                    .isPresent()) {
                  TreeTransformerUtil.deleteNode(context);
                }
                return TraversalControl.CONTINUE;
              }
            });
  }
}
