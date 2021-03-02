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

import com.apollographql.federation.graphqljava.Federation;
import com.apollographql.federation.graphqljava._FieldSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.GraphqlErrorException;
import graphql.language.Argument;
import graphql.language.Directive;
import graphql.language.EnumTypeDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
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
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.scalars.CqlScalar;
import io.stargate.graphql.schema.schemafirst.fetchers.dynamic.FederatedEntity;
import io.stargate.graphql.schema.schemafirst.fetchers.dynamic.FederatedEntityFetcher;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SchemaProcessor {

  private static final TypeDefinitionRegistry CQL_DIRECTIVES =
      new SchemaParser()
          .parse(
              new InputStreamReader(
                  SchemaProcessor.class.getResourceAsStream("/schemafirst/cql_directives.graphql"),
                  StandardCharsets.UTF_8));

  private static final TypeDefinitionRegistry FEDERATION_DIRECTIVES =
      new SchemaParser()
          .parse(
              new InputStreamReader(
                  SchemaProcessor.class.getResourceAsStream("/schemafirst/federation.graphql"),
                  StandardCharsets.UTF_8));

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;
  private final boolean isPersisted;

  /**
   * @param isPersisted whether we are processing a schema already stored in the database, or a
   *     schema that a user is attempting to deploy. This is just used to customize a couple of
   *     error messages.
   */
  public SchemaProcessor(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory,
      boolean isPersisted) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
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
  public ProcessedSchema process(String source, Keyspace keyspace) {
    TypeDefinitionRegistry registry = parse(source);
    ProcessingContext context = new ProcessingContext(registry, keyspace, isPersisted);
    MappingModel mappingModel = MappingModel.build(registry, context);

    addGeneratedTypes(mappingModel, registry);

    GraphQL graphql = buildGraphql(registry, mappingModel, context.getUsedCqlScalars());

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
      TypeDefinitionRegistry registry, MappingModel mappingModel, EnumSet<CqlScalar> cqlScalars) {

    // SchemaGenerator fails if the schema uses directives that are not defined. Add everything we
    // need:
    registry = registry.merge(FEDERATION_DIRECTIVES); // GraphQL federation
    registry = registry.merge(CQL_DIRECTIVES); // Stargate's own CQL directives

    finalizeKeyDirectives(registry, mappingModel);

    RuntimeWiring.Builder runtimeWiring =
        RuntimeWiring.newRuntimeWiring()
            .codeRegistry(buildCodeRegistry(mappingModel))
            .scalar(_FieldSet.type);
    for (CqlScalar cqlScalar : cqlScalars) {
      runtimeWiring.scalar(cqlScalar.getGraphqlType());
    }
    GraphQLSchema schema =
        new SchemaGenerator()
            .makeExecutableSchema(
                SchemaGenerator.Options.defaultOptions(), registry, runtimeWiring.build());

    // However once we have the schema we don't need the CQL directives anymore: they only impact
    // the queries we generate, they're not useful for users of the schema.
    schema = removeCqlDirectives(schema);

    com.apollographql.federation.graphqljava.SchemaTransformer federationTransformer =
        Federation.transform(schema);
    if (mappingModel.hasFederatedEntities()) {
      federationTransformer
          .fetchEntities(
              new FederatedEntityFetcher(
                  mappingModel, authenticationService, authorizationService, dataStoreFactory))
          .resolveEntityType(
              environment -> {
                FederatedEntity entity = environment.getObject();
                return environment.getSchema().getObjectType(entity.getTypeName());
              });
    }
    schema = federationTransformer.build();

    return GraphQL.newGraphQL(schema).build();
  }

  // For convenience we make users not fill `@key.fields`: it will always be the CQL primary key, so
  // we can infer it. However it is a required field, so fill it now before we try to validate the
  // schema.
  private void finalizeKeyDirectives(TypeDefinitionRegistry registry, MappingModel mappingModel) {
    if (!mappingModel.hasFederatedEntities()) {
      return;
    }
    for (EntityModel entity : mappingModel.getEntities().values()) {
      if (!entity.isFederated()) {
        continue;
      }
      Optional<ObjectTypeDefinition> maybeType =
          registry.getType(entity.getGraphqlName(), ObjectTypeDefinition.class);
      assert maybeType.isPresent(); // because we built an EntityMappingModel from it
      ObjectTypeDefinition type = maybeType.get();

      String primaryKeyFields =
          entity.getPrimaryKey().stream()
              .map(FieldModel::getGraphqlName)
              .collect(Collectors.joining(" "));
      Directive newKeyDirective =
          Directive.newDirective()
              .name("key")
              .argument(
                  Argument.newArgument(
                          "fields", StringValue.newStringValue(primaryKeyFields).build())
                      .build())
              .build();
      List<Directive> newDirectives = Lists.newArrayListWithCapacity(type.getDirectives().size());
      newDirectives.add(newKeyDirective);
      // Other directives are unchanged. Note: this assumes that there is only one @key directive.
      // This is true now, but if we allow multiple @key in the future this will have to be
      // revisited.
      for (Directive directive : type.getDirectives()) {
        if (!directive.getName().equals("key")) {
          newDirectives.add(directive);
        }
      }
      ObjectTypeDefinition newType = type.transform(builder -> builder.directives(newDirectives));
      registry.remove(type);
      registry.add(newType);
    }
  }

  private TypeDefinitionRegistry parse(String source) throws GraphqlErrorException {
    SchemaParser parser = new SchemaParser();
    try {
      return parser.parse(source);
    } catch (SchemaProblem schemaProblem) {
      List<GraphQLError> schemaErrors = schemaProblem.getErrors();
      String schemaOrigin = isPersisted ? "stored for this namespace" : "that you provided";
      throw GraphqlErrorException.newErrorException()
          .message(
              String.format(
                  "The schema %s is not valid GraphQL. See details in `extensions.schemaErrors` below.",
                  schemaOrigin))
          .extensions(ImmutableMap.of("schemaErrors", schemaErrors))
          .build();
    }
  }

  private GraphQLCodeRegistry buildCodeRegistry(MappingModel mappingModel) {
    GraphQLCodeRegistry.Builder builder = GraphQLCodeRegistry.newCodeRegistry();
    for (OperationModel operation : mappingModel.getOperations()) {
      builder.dataFetcher(
          operation.getCoordinates(),
          operation.getDataFetcher(
              mappingModel, authenticationService, authorizationService, dataStoreFactory));
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
                if (CQL_DIRECTIVES
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
                if (CQL_DIRECTIVES.getDirectiveDefinition(node.getName()).isPresent()) {
                  TreeTransformerUtil.deleteNode(context);
                }
                return TraversalControl.CONTINUE;
              }
            });
  }
}
