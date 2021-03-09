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

import com.google.common.collect.ImmutableMap;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.GraphqlErrorException;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.errors.SchemaProblem;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
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
    registry.merge(CQL_DIRECTIVES);
    SchemaGenerator schemaGenerator = new SchemaGenerator();
    GraphQLSchema schema =
        schemaGenerator.makeExecutableSchema(
            SchemaGenerator.Options.defaultOptions(),
            registry,
            RuntimeWiring.newRuntimeWiring().codeRegistry(buildCodeRegistry(mappingModel)).build());
    return GraphQL.newGraphQL(schema).build();
  }

  private GraphQLCodeRegistry buildCodeRegistry(MappingModel mappingModel) {
    GraphQLCodeRegistry.Builder builder = GraphQLCodeRegistry.newCodeRegistry();
    for (QueryMappingModel query : mappingModel.getQueries()) {
      builder.dataFetcher(
          query.getCoordinates(),
          query.buildDataFetcher(authenticationService, authorizationService, dataStoreFactory));
    }
    // TODO also handle mutations
    return builder.build();
  }

  private TypeDefinitionRegistry parse(String source) throws GraphqlErrorException {
    graphql.schema.idl.SchemaParser parser = new graphql.schema.idl.SchemaParser();
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

  // TODO make private (+ move to a file?)
  public static final TypeDefinitionRegistry CQL_DIRECTIVES =
      new SchemaParser()
          .parse(
              "\"The type of schema element a GraphQL object maps to\" "
                  + "enum EntityTarget { TABLE UDT } "
                  + "\"Customizes the mapping of a GraphQL object to a CQL table or UDT\""
                  + "directive @cqlEntity( "
                  + "  \"A custom table or UDT name (otherwise it uses the same name as the object)\" "
                  + "  name: String "
                  + "  \"Whether the object maps to a CQL table (the default) or UDT\" "
                  + "  target: EntityTarget "
                  + ") on OBJECT "
                  + "\"The sorting order for clustering columns\" "
                  + "enum ClusteringOrder { ASC DESC } "
                  + "\"Customizes the mapping of a GraphQL field to a CQL column (or UDT field)\""
                  + "directive @cqlColumn( "
                  + "  \"A custom column name (otherwise it uses the same name as the field)\" "
                  + "  name: String "
                  + "  \"Whether the column forms part of the partition key\" "
                  + "  partitionKey: Boolean "
                  + "  \"Whether the column is a clustering column, and if so in which order.\""
                  + "  clusteringOrder: ClusteringOrder "
                  + "  \"The CQL type to map to (e.g. frozen<list<varchar>>)\" "
                  + "  type: String "
                  + ") on FIELD_DEFINITION");
}
