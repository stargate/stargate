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
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.GraphqlErrorException;
import graphql.language.EnumTypeDefinition;
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
import io.stargate.graphql.schema.schemafirst.fetchers.generated.FederatedEntity;
import io.stargate.graphql.schema.schemafirst.fetchers.generated.FederatedEntityFetcher;
import io.stargate.graphql.schema.schemafirst.fetchers.generated.QueryFetcher;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SchemaProcessor {

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;

  public SchemaProcessor(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
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
    ProcessingContext context = new ProcessingContext(registry, keyspace);
    MappingModel mappingModel = MappingModel.build(registry, context);

    GraphQL graphql = buildGraphql(registry, mappingModel);

    return new ProcessedSchema(graphql, mappingModel, context.getMessages());
  }

  private GraphQL buildGraphql(TypeDefinitionRegistry registry, MappingModel mappingModel) {

    // SchemaGenerator fails if the schema uses directives that are not defined. Add everything we
    // need:
    registry = registry.merge(FEDERATION_DIRECTIVES); // GraphQL federation
    registry = registry.merge(CQL_DIRECTIVES); // Stargate's own CQL directives

    GraphQLSchema schema =
        new SchemaGenerator()
            .makeExecutableSchema(
                SchemaGenerator.Options.defaultOptions(),
                registry,
                RuntimeWiring.newRuntimeWiring()
                    .codeRegistry(buildCodeRegistry(mappingModel))
                    .scalar(_FieldSet.type)
                    .build());

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

  private TypeDefinitionRegistry parse(String source) throws GraphqlErrorException {
    SchemaParser parser = new SchemaParser();
    try {
      return parser.parse(source);
    } catch (SchemaProblem schemaProblem) {
      List<GraphQLError> schemaErrors = schemaProblem.getErrors();
      throw GraphqlErrorException.newErrorException()
          .message(
              "The schema you provided is not valid GraphQL. "
                  + "See details in `extensions.schemaErrors` below.")
          .extensions(ImmutableMap.of("schemaErrors", schemaErrors))
          .build();
    }
  }

  private GraphQLCodeRegistry buildCodeRegistry(MappingModel mappingModel) {
    GraphQLCodeRegistry.Builder builder = GraphQLCodeRegistry.newCodeRegistry();
    for (QueryMappingModel model : mappingModel.getQueries()) {
      builder.dataFetcher(
          model.getCoordinates(),
          new QueryFetcher(model, authenticationService, authorizationService, dataStoreFactory));
    }
    // TODO also handle mutations
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

  // TODO make private
  public static final TypeDefinitionRegistry CQL_DIRECTIVES =
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
}
