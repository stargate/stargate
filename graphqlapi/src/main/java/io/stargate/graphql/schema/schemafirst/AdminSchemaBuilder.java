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
package io.stargate.graphql.schema.schemafirst;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.FieldCoordinates.coordinates;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLCodeRegistry.newCodeRegistry;
import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLEnumValueDefinition.newEnumValueDefinition;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLObjectType.newObject;
import static graphql.schema.GraphQLScalarType.newScalar;
import static graphql.schema.GraphQLSchema.newSchema;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.schemafirst.fetchers.admin.*;
import io.stargate.graphql.schema.schemafirst.migration.MigrationStrategy;
import io.stargate.graphql.web.resources.GraphqlResourceBase;
import java.io.InputStream;

public class AdminSchemaBuilder {

  private static final GraphQLObjectType DC_TYPE =
      newObject()
          .name("Datacenter")
          .description(
              "A group of Cassandra nodes, related and configured within a cluster for replication purposes.")
          .field(
              newFieldDefinition()
                  .name("name")
                  .description("The name of the datacenter.")
                  .type(nonNull(GraphQLString))
                  .build())
          .field(
              newFieldDefinition()
                  .name("replicas")
                  .description(
                      "The number of data replicas that Cassandra uses for this datacenter.")
                  .type(nonNull(GraphQLInt))
                  .build())
          .build();

  private static final GraphQLInputObjectType DC_INPUT_TYPE =
      newInputObject()
          .name("DatacenterInput")
          .description(
              "A group of Cassandra nodes, related and configured within a cluster for replication purposes.")
          .field(
              newInputObjectField()
                  .name("name")
                  .description("The name of the datacenter.")
                  .type(nonNull(GraphQLString))
                  .build())
          .field(
              newInputObjectField()
                  .name("replicas")
                  .description(
                      "The number of data replicas that Cassandra uses for this datacenter.")
                  .defaultValue(3)
                  .type(GraphQLInt)
                  .build())
          .build();

  private static final GraphQLObjectType NAMESPACE_TYPE =
      newObject()
          .name("Namespace")
          .description(
              "A logical grouping of all the types in a GraphQL schema.\n"
                  + "(Note: there is a 1:1 mapping between namespaces and Cassandra keyspaces.)")
          .field(
              newFieldDefinition()
                  .name("name")
                  .description("The name of the namespace.")
                  .type(nonNull(GraphQLString))
                  .build())
          .field(
              newFieldDefinition()
                  .name("replicas")
                  .description(
                      "The number of replicas if the namespace uses the simple replication strategy "
                          + "(exactly one of `replicas` or `datacenters` will be present).")
                  .type(GraphQLInt)
                  .build())
          .field(
              newFieldDefinition()
                  .name("datacenters")
                  .description(
                      "The number of replicas per datacenter, if the namespace uses the network "
                          + "topology replication strategy "
                          + "(exactly one of `replicas` or `datacenters` will be present).")
                  .type(list(DC_TYPE))
                  .build())
          .build();

  private static final GraphQLFieldDefinition NAMESPACE_QUERY =
      newFieldDefinition()
          .name("namespace")
          .description("Lists a single namespace")
          .argument(
              newArgument()
                  .name("name")
                  .description("The name of the namespace")
                  .type(nonNull(GraphQLString))
                  .build())
          .type(NAMESPACE_TYPE)
          .build();

  private static final GraphQLFieldDefinition NAMESPACES_QUERY =
      newFieldDefinition()
          .name("namespaces")
          .description("Lists all available namespaces")
          .type(list(NAMESPACE_TYPE))
          .build();

  private static final GraphQLObjectType SCHEMA_TYPE =
      newObject()
          .name("Schema")
          .description("The GraphQL schema that is deployed in a namespace")
          .field(
              newFieldDefinition()
                  .name("namespace")
                  .description("The name of the namespace.")
                  .type(nonNull(GraphQLString))
                  .build())
          .field(
              newFieldDefinition()
                  .name("version")
                  .description("The version of the schema.")
                  .type(GraphQLString)
                  .build())
          .field(
              newFieldDefinition()
                  .name("deployDate")
                  .description("The date that this version was deployed.")
                  .type(GraphQLString)
                  .build())
          .field(
              newFieldDefinition()
                  .name("contents")
                  .description(
                      "The contents that were provided to the `deploySchema` mutation.\n"
                          + "Note that this is a partial schema: the actual schema used at "
                          + "runtime is augmented with directive definitions, generated types, "
                          + "etc.")
                  .type(GraphQLString)
                  .build())
          // TODO add 'uri' field to download as a file
          .build();

  private static final GraphQLFieldDefinition SCHEMA_QUERY =
      newFieldDefinition()
          .name("schema")
          .description("Retrieves the GraphQL schema that is deployed in a namespace")
          .argument(
              newArgument()
                  .name("namespace")
                  .description("The name of the namespace")
                  .type(nonNull(GraphQLString))
                  .build())
          .argument(
              newArgument()
                  .name("version")
                  .description(
                      "The version of the schema to get. If not specified, it will return the latest version.")
                  .type(GraphQLString))
          .type(nonNull(SCHEMA_TYPE))
          .build();

  private static final GraphQLFieldDefinition SCHEMA_HISTORY_PER_NAMESPACE_QUERY =
      newFieldDefinition()
          .name("schemas")
          .description("Retrieves all GraphQL schemas that are deployed in a namespace")
          .argument(
              newArgument()
                  .name("namespace")
                  .description("The name of the namespace")
                  .type(nonNull(GraphQLString))
                  .build())
          .type(list(SCHEMA_TYPE))
          .build();

  private static final GraphQLObjectType QUERY =
      newObject()
          .name("Query")
          .field(NAMESPACES_QUERY)
          .field(NAMESPACE_QUERY)
          .field(SCHEMA_QUERY)
          .field(SCHEMA_HISTORY_PER_NAMESPACE_QUERY)
          .build();

  // See https://graphql-rules.com/rules/mutation-payload-query
  private static GraphQLFieldDefinition QUERY_FIELD =
      newFieldDefinition()
          .name("query")
          .description(
              "A reference to the `Query` object, "
                  + "in case you need to chain queries after the mutation.")
          .type(QUERY)
          .build();

  private static final GraphQLObjectType CREATE_NAMESPACE_TYPE =
      newObject()
          .name("CreateNamespaceResponse")
          .description("The outcome of a `createNamespace` mutation")
          .field(
              newFieldDefinition()
                  .name("namespace")
                  .description("The namespace that was created, or already existed.")
                  .type(NAMESPACE_TYPE)
                  .build())
          .field(QUERY_FIELD)
          .build();

  private static final GraphQLFieldDefinition CREATE_NAMESPACE_MUTATION =
      newFieldDefinition()
          .name("createNamespace")
          .description("Creates a namespace where a new GraphQL schema can be installed.")
          .argument(
              newArgument()
                  .name("name")
                  .description("The name of the namespace")
                  .type(nonNull(GraphQLString))
                  .build())
          .argument(
              newArgument()
                  .name("replicas")
                  .description(
                      "The number of replicas if the namespace is to use the simple "
                          + "replication strategy (defaults to 1 if "
                          + "`datacenters` is not provided either).")
                  .type(GraphQLInt)
                  .build())
          .argument(
              newArgument()
                  .name("datacenters")
                  .description(
                      "The number of replicas per datacenter if the namespace is to "
                          + "use the network topology replication strategy.")
                  .type(list(DC_INPUT_TYPE))
                  .build())
          .argument(
              newArgument()
                  .name("ifNotExists")
                  .description(
                      "What to do if the namespace already exists: "
                          + "if `true`, the mutation will succeed; "
                          + "if `false` (the default), an error will be raised.")
                  .defaultValue(false)
                  .type(GraphQLBoolean)
                  .build())
          .type(nonNull(CREATE_NAMESPACE_TYPE))
          .build();

  private static final GraphQLObjectType DROP_NAMESPACE_TYPE =
      newObject()
          .name("DropNamespaceResponse")
          .description("The outcome of a `dropNamespace` mutation")
          .field(
              newFieldDefinition()
                  .name("applied")
                  .description(
                      "Will always be `true` (an error is always raised if the deletion failed).")
                  .type(nonNull(GraphQLBoolean))
                  .build())
          .field(QUERY_FIELD)
          .build();

  private static final GraphQLFieldDefinition DROP_NAMESPACE_MUTATION =
      newFieldDefinition()
          .name("dropNamespace")
          .description("Drops a namespace.")
          .argument(
              newArgument()
                  .name("name")
                  .description("The name of the namespace")
                  .type(nonNull(GraphQLString))
                  .build())
          .argument(
              newArgument()
                  .name("ifExists")
                  .description(
                      "What to do if the namespace does not exists: "
                          + "if `true`, the mutation will succeed; "
                          + "if `false` (the default), an error will be raised.")
                  .defaultValue(false)
                  .type(GraphQLBoolean)
                  .build())
          .type(nonNull(DROP_NAMESPACE_TYPE))
          .build();

  public static final GraphQLEnumType MIGRATION_STRATEGY_ENUM =
      newEnum()
          .name("MigrationStrategy")
          .description(
              "What to do if the CQL schema doesn't match the provided GraphQL schema. "
                  + "Note that a table will be considered matching even if it contains additional "
                  + "columns not present in the GraphQL schema. In other words, a deploy "
                  + "operation will never drop columns.")
          .value(
              newEnumValueDefinition()
                  .name(MigrationStrategy.USE_EXISTING.name())
                  .value(MigrationStrategy.USE_EXISTING)
                  .description(
                      "Don't do anything. All CQL tables and UDTs must match, otherwise the "
                          + "deployment is aborted. This is the most conservative strategy.")
                  .build())
          .value(
              newEnumValueDefinition()
                  .name(MigrationStrategy.ADD_MISSING_TABLES.name())
                  .value(MigrationStrategy.ADD_MISSING_TABLES)
                  .description(
                      "Create CQL tables and UDTs that don't already exist. "
                          + "Those that exist must match, otherwise the deployment is aborted.")
                  .build())
          .value(
              newEnumValueDefinition()
                  .name(MigrationStrategy.ADD_MISSING_TABLES_AND_COLUMNS.name())
                  .value(MigrationStrategy.ADD_MISSING_TABLES_AND_COLUMNS)
                  .description(
                      "Create CQL tables and UDTs that don't already exist. "
                          + "For those that exist, add any missing columns (as long as they're "
                          + "not marked as partition key or clustering). Note that this could "
                          + "still fail if the column existed before with a different type.")
                  .build())
          .value(
              newEnumValueDefinition()
                  .name(MigrationStrategy.DROP_AND_RECREATE_ALL.name())
                  .value(MigrationStrategy.DROP_AND_RECREATE_ALL)
                  .description(
                      "Drop and recreate all CQL tables and UDTs. "
                          + "This is a destructive operation: any existing data will be lost.")
                  .build())
          .value(
              newEnumValueDefinition()
                  .name(MigrationStrategy.DROP_AND_RECREATE_IF_MISMATCH.name())
                  .value(MigrationStrategy.DROP_AND_RECREATE_IF_MISMATCH)
                  .description(
                      "Drop and recreate only the CQL tables and UDTs that don't match. "
                          + "This is a destructive operation: any existing data in those tables "
                          + "will be lost (however, tables that didn't change will keep their "
                          + "data).")
                  .build())
          .build();

  private static final GraphQLEnumType MESSAGE_CATEGORY_ENUM =
      newEnum()
          .name("MessageCategory")
          .description("The categories of messages returned by deploy mutations.")
          .value("INFO")
          .value("WARNING")
          .build();

  private static final GraphQLObjectType LOCATION_TYPE =
      newObject()
          .name("Location")
          .description("A location in the GraphQL schema passed to a deploy mutation.")
          .field(newFieldDefinition().name("line").type(GraphQLInt).build())
          .field(newFieldDefinition().name("column").type(GraphQLInt).build())
          .build();

  private static final GraphQLObjectType MESSAGE_TYPE =
      newObject()
          .name("Message")
          .description("A message returned by a deploy mutation.")
          .field(newFieldDefinition().name("contents").type(nonNull(GraphQLString)).build())
          .field(newFieldDefinition().name("category").type(nonNull(MESSAGE_CATEGORY_ENUM)).build())
          .field(newFieldDefinition().name("locations").type(list(LOCATION_TYPE)).build())
          .build();

  private static final GraphQLObjectType DEPLOY_SCHEMA_TYPE =
      newObject()
          .name("DeploySchemaResponse")
          .description("The outcome of a `deploySchema` mutation")
          .field(
              newFieldDefinition()
                  .name("version")
                  .description("The new schema version that was created by this operation.")
                  .type(GraphQLString)
                  .build())
          .field(
              newFieldDefinition()
                  .name("messages")
                  .description(
                      "Warnings or other notices reported during deployment.\n"
                          + "Note that errors are reported as a GraphQL error, not here.")
                  // TODO add an argument to filter by category
                  // currently blocked by https://github.com/graphql-java/graphql-java/pull/2126
                  .type(list(MESSAGE_TYPE))
                  .build())
          .field(
              newFieldDefinition()
                  .name("cqlChanges")
                  .description(
                      "The queries that were executed to adapt the CQL data model to the new "
                          + "schema.")
                  .type(list(GraphQLString))
                  .build())
          .field(QUERY_FIELD)
          .build();

  private static GraphQLFieldDefinition.Builder deploySchemaStart() {
    return newFieldDefinition()
        .argument(
            newArgument()
                .name("namespace")
                .description("The namespace to deploy to.")
                .type(nonNull(GraphQLString))
                .build())
        .argument(
            newArgument()
                .name("expectedVersion")
                .description(
                    "The version that those changes apply to, "
                        + "or null if this is the first deployment.\n"
                        + "This is used to prevent concurrent updates.")
                .type(GraphQLString)
                .build())
        .argument(
            newArgument()
                .name("migrationStrategy")
                .description("The strategy to update the CQL schema if necessary.")
                .type(nonNull(MIGRATION_STRATEGY_ENUM))
                .defaultValue(MigrationStrategy.ADD_MISSING_TABLES_AND_COLUMNS)
                .build())
        .argument(
            newArgument()
                .name("dryRun")
                .description(
                    "Just parse and validate the schema, don't deploy it or apply any changes to "
                        + "the database. This is useful in particular in conjunction with the "
                        + "`cqlChanges` field in the result, to evaluate the impacts on the CQL "
                        + "data model.")
                .type(nonNull(GraphQLBoolean))
                .defaultValue(false)
                .build())
        .type(DEPLOY_SCHEMA_TYPE);
  }

  private static final GraphQLFieldDefinition DEPLOY_SCHEMA_MUTATION =
      deploySchemaStart()
          .name("deploySchema")
          .description("Deploys a GraphQL schema to a namespace.")
          .argument(
              newArgument()
                  .name("schema")
                  .description("The contents of the schema.")
                  .type(nonNull(GraphQLString))
                  .build())
          .build();

  private static final GraphQLScalarType UPLOAD_SCALAR =
      newScalar().name("Upload").coercing(new InputStreamCoercing()).build();

  private static final GraphQLFieldDefinition DEPLOY_SCHEMA_FILE_MUTATION =
      deploySchemaStart()
          .name("deploySchemaFile")
          .description(
              "Deploys a GraphQL schema to a namespace via a file upload.\n"
                  + "This mutation must be executed with a [multipart request](https://github.com/jaydenseric/graphql-multipart-request-spec) "
                  + "(note that your `operations` part **must** declare MIME type `application/json`).")
          .argument(
              newArgument()
                  .name("schemaFile")
                  .description("The contents of the schema as an UTF-8 encoded file.")
                  .type(nonNull(UPLOAD_SCALAR))
                  .build())
          .build();

  private static final GraphQLObjectType MUTATION =
      newObject()
          .name("Mutation")
          .field(CREATE_NAMESPACE_MUTATION)
          .field(DROP_NAMESPACE_MUTATION)
          .field(DEPLOY_SCHEMA_MUTATION)
          .field(DEPLOY_SCHEMA_FILE_MUTATION)
          .build();

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;

  public AdminSchemaBuilder(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
  }

  public GraphQLSchema build() {
    return newSchema()
        .query(QUERY)
        .mutation(MUTATION)
        .codeRegistry(
            newCodeRegistry()
                .dataFetcher(
                    coordinates(QUERY, NAMESPACES_QUERY),
                    new AllNamespacesFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(QUERY, NAMESPACE_QUERY),
                    new SingleNamespaceFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(QUERY, SCHEMA_QUERY),
                    new SingleSchemaFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(QUERY, SCHEMA_HISTORY_PER_NAMESPACE_QUERY),
                    new AllSchemasFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(MUTATION, CREATE_NAMESPACE_MUTATION),
                    new CreateNamespaceFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(MUTATION, DROP_NAMESPACE_MUTATION),
                    new DropNamespaceFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(MUTATION, DEPLOY_SCHEMA_MUTATION),
                    new DeploySchemaFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .dataFetcher(
                    coordinates(MUTATION, DEPLOY_SCHEMA_FILE_MUTATION),
                    new DeploySchemaFileFetcher(
                        authenticationService, authorizationService, dataStoreFactory))
                .build())
        .build();
  }

  /** See the multipart method in {@link GraphqlResourceBase}. */
  private static class InputStreamCoercing implements Coercing<InputStream, Void> {
    @Override
    public Void serialize(Object dataFetcherResult) throws CoercingSerializeException {
      throw new CoercingSerializeException("Upload can only be used for input");
    }

    @Override
    public InputStream parseValue(Object input) throws CoercingParseValueException {
      if (input instanceof InputStream) {
        return ((InputStream) input);
      } else if (input == null) {
        return null;
      } else {
        throw new CoercingParseValueException(
            String.format("Expected a FileInputStream, got %s", input.getClass().getSimpleName()));
      }
    }

    @Override
    public InputStream parseLiteral(Object input) throws CoercingParseLiteralException {
      throw new CoercingParseLiteralException("Upload values must be specified as JSON variables");
    }
  }
}
