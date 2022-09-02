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
package io.stargate.sgv2.graphql.schema.graphqlfirst;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.FieldCoordinates.coordinates;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLCodeRegistry.newCodeRegistry;
import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLEnumValueDefinition.newEnumValueDefinition;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
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
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin.AllSchemasFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin.DeploySchemaFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin.DeploySchemaFileFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin.SingleSchemaFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin.UndeploySchemaFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.MigrationStrategy;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ProcessingLogType;
import io.stargate.sgv2.graphql.web.resources.GraphqlResourceBase;
import io.stargate.sgv2.graphql.web.resources.ResourcePaths;
import java.io.InputStream;

public class AdminSchemaBuilder {

  private static final GraphQLObjectType SCHEMA_TYPE =
      newObject()
          .name("Schema")
          .description("The GraphQL schema that is deployed in a keyspace")
          .field(
              newFieldDefinition()
                  .name("keyspace")
                  .description("The name of the keyspace.")
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
          .field(
              newFieldDefinition()
                  .name("contentsUri")
                  .description(
                      String.format(
                          "A link to download `contents` as a file.%n"
                              + "It is relative to the URI (ending in %s) that was used to "
                              + "execute this GraphQL request.",
                          ResourcePaths.ADMIN))
                  .type(GraphQLString)
                  .build())
          .build();

  private static final GraphQLFieldDefinition SCHEMA_QUERY =
      newFieldDefinition()
          .name("schema")
          .description("Retrieves the GraphQL schema that is deployed in a keyspace")
          .argument(
              newArgument()
                  .name("keyspace")
                  .description("The name of the keyspace")
                  .type(nonNull(GraphQLString))
                  .build())
          .argument(
              newArgument()
                  .name("version")
                  .description(
                      "The version of the schema to get. If not specified, it will return the latest version.")
                  .type(GraphQLString))
          .type(SCHEMA_TYPE)
          .build();

  private static final GraphQLFieldDefinition SCHEMA_HISTORY_PER_KEYSPACE_QUERY =
      newFieldDefinition()
          .name("schemas")
          .description("Retrieves all GraphQL schemas that are deployed in a keyspace")
          .argument(
              newArgument()
                  .name("keyspace")
                  .description("The name of the keyspace")
                  .type(nonNull(GraphQLString))
                  .build())
          .type(list(SCHEMA_TYPE))
          .build();

  private static final GraphQLObjectType QUERY =
      newObject()
          .name("Query")
          .field(SCHEMA_QUERY)
          .field(SCHEMA_HISTORY_PER_KEYSPACE_QUERY)
          .build();

  // See https://graphql-rules.com/rules/mutation-payload-query
  private static final GraphQLFieldDefinition QUERY_FIELD =
      newFieldDefinition()
          .name("query")
          .description(
              "A reference to the `Query` object, "
                  + "in case you need to chain queries after the mutation.")
          .type(QUERY)
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

  private static final GraphQLEnumType LOG_CATEGORY_ENUM =
      newEnum()
          .name("LogCategory")
          .description("The categories of logs emitted by deploy mutations.")
          .value(
              newEnumValueDefinition()
                  .name(ProcessingLogType.Info.name().toUpperCase())
                  .value(ProcessingLogType.Info)
                  .build())
          .value(
              newEnumValueDefinition()
                  .name(ProcessingLogType.Warning.name().toUpperCase())
                  .value(ProcessingLogType.Warning)
                  .build())
          .build();

  private static final GraphQLObjectType LOCATION_TYPE =
      newObject()
          .name("Location")
          .description("A location in the GraphQL schema passed to a deploy mutation.")
          .field(newFieldDefinition().name("line").type(GraphQLInt).build())
          .field(newFieldDefinition().name("column").type(GraphQLInt).build())
          .build();

  private static final GraphQLObjectType LOG_TYPE =
      newObject()
          .name("Log")
          .description("A log emitted by a deploy mutation.")
          .field(newFieldDefinition().name("message").type(nonNull(GraphQLString)).build())
          .field(newFieldDefinition().name("category").type(nonNull(LOG_CATEGORY_ENUM)).build())
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
                  .name("logs")
                  .description(
                      "Warnings or info logs emitted during the deployment.\n"
                          + "Note that errors are reported as a GraphQL error, not here.")
                  .argument(
                      newArgument()
                          .name("category")
                          .description("Filter the logs to a particular category")
                          .type(LOG_CATEGORY_ENUM)
                          .build())
                  .type(list(LOG_TYPE))
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

  private static final GraphQLFieldDefinition DEPLOY_SCHEMA_MUTATION =
      deploySchemaStart()
          .name("deploySchema")
          .description("Deploys a GraphQL schema to a keyspace.")
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
              "Deploys a GraphQL schema to a keyspace via a file upload.\n"
                  + "This mutation must be executed with a [multipart request](https://github.com/jaydenseric/graphql-multipart-request-spec) "
                  + "(note that your `operations` part **must** declare MIME type `application/json`).")
          .argument(
              newArgument()
                  .name("schemaFile")
                  .description("The contents of the schema as an UTF-8 encoded file.")
                  .type(nonNull(UPLOAD_SCALAR))
                  .build())
          .build();

  private static final GraphQLFieldDefinition UNDEPLOY_SCHEMA_MUTATION =
      newFieldDefinition()
          .name("undeploySchema")
          .description(
              "Cancels a previous deployment.\n"
                  + "The keyspace will revert to the generated, \"CQL-first\" schema. The schema history will be "
                  + "preserved, but with all versions marked as `current: false`.")
          .argument(
              newArgument()
                  .name("keyspace")
                  .description("The keyspace to deploy to.")
                  .type(nonNull(GraphQLString))
                  .build())
          .argument(
              newArgument()
                  .name("expectedVersion")
                  .description(
                      "The current version.\nThis is used to ensure that another user is not deploying concurrently.")
                  .type(nonNull(GraphQLString))
                  .build())
          .argument(
              newArgument()
                  .name("force")
                  .description(
                      "Proceed even if the previous deployment is still marked as in progress. "
                          + "This is used to recover manually if a previous deployment failed unexpectedly during the "
                          + "CQL migration phase.")
                  .type(nonNull(GraphQLBoolean))
                  .defaultValue(false)
                  .build())
          .type(GraphQLBoolean)
          .build();

  private static final GraphQLObjectType MUTATION =
      newObject()
          .name("Mutation")
          .field(DEPLOY_SCHEMA_MUTATION)
          .field(DEPLOY_SCHEMA_FILE_MUTATION)
          .field(UNDEPLOY_SCHEMA_MUTATION)
          .build();

  private static GraphQLFieldDefinition.Builder deploySchemaStart() {
    return newFieldDefinition()
        .argument(
            newArgument()
                .name("keyspace")
                .description("The keyspace to deploy to.")
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
                .name("force")
                .description(
                    "Proceed even if the previous deployment is still marked as in progress. "
                        + "This is used to recover manually if a previous deployment failed unexpectedly during the "
                        + "CQL migration phase.")
                .type(nonNull(GraphQLBoolean))
                .defaultValue(false)
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

  public GraphQLSchema build() {
    return newSchema()
        .query(QUERY)
        .mutation(MUTATION)
        .codeRegistry(
            newCodeRegistry()
                .dataFetcher(coordinates(QUERY, SCHEMA_QUERY), new SingleSchemaFetcher())
                .dataFetcher(
                    coordinates(QUERY, SCHEMA_HISTORY_PER_KEYSPACE_QUERY), new AllSchemasFetcher())
                .dataFetcher(
                    coordinates(MUTATION, DEPLOY_SCHEMA_MUTATION), new DeploySchemaFetcher())
                .dataFetcher(
                    coordinates(MUTATION, DEPLOY_SCHEMA_FILE_MUTATION),
                    new DeploySchemaFileFetcher())
                .dataFetcher(
                    coordinates(MUTATION, UNDEPLOY_SCHEMA_MUTATION), new UndeploySchemaFetcher())
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
