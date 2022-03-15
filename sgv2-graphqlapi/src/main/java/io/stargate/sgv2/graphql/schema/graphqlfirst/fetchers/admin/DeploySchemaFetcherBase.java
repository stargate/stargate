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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.CassandraMigrator;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.MigrationQuery;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.MigrationStrategy;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ProcessedSchema;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.SchemaProcessor;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

abstract class DeploySchemaFetcherBase extends CassandraFetcher<DeploySchemaResponseDto> {

  @Override
  protected DeploySchemaResponseDto get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception {

    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(context.getBridge());

    String keyspaceName = environment.getArgument("keyspace");
    CqlKeyspaceDescribe keyspace =
        context
            .getBridge()
            .getKeyspace(keyspaceName, true)
            .orElseThrow(() -> new IllegalArgumentException("Keyspace '%s' does not exist."));

    String input = getSchemaContents(environment);
    UUID expectedVersion = getExpectedVersion(environment);
    MigrationStrategy migrationStrategy = environment.getArgument("migrationStrategy");
    boolean force = environment.getArgument("force");
    boolean dryRun = environment.getArgument("dryRun");
    if (!dryRun) {
      schemaSourceDao.startDeployment(keyspaceName, expectedVersion, force);
    }

    DeploySchemaResponseDto response = new DeploySchemaResponseDto();
    List<MigrationQuery> queries;
    ProcessedSchema processedSchema;
    try {
      processedSchema = new SchemaProcessor(context.getBridge(), false).process(input, keyspace);
      response.setLogs(processedSchema.getLogs());

      queries =
          CassandraMigrator.forDeployment(migrationStrategy)
              .compute(processedSchema.getMappingModel(), keyspace);

      response.setCqlChanges(queries);
    } catch (Exception e) {
      if (!dryRun) {
        schemaSourceDao.abortDeployment(keyspaceName); // unsets the flag (no need for LWT)
      }
      throw e;
    }

    if (!dryRun) {
      for (MigrationQuery query : queries) {
        context.getBridge().executeQuery(query.build());
      }
      SchemaSource newSource = schemaSourceDao.insert(keyspaceName, input);
      schemaSourceDao.purgeOldVersions(keyspaceName);
      response.setVersion(newSource.getVersion());

      // Reload the keyspace because we want the latest hash when updating in the cache
      keyspace =
          context
              .getBridge()
              .getKeyspace(keyspaceName, false)
              .orElseThrow(() -> new IllegalArgumentException("Keyspace '%s' does not exist."));
      context.getGraphqlCache().putDml(keyspace, newSource, processedSchema.getGraphql());
    }
    return response;
  }

  protected abstract String getSchemaContents(DataFetchingEnvironment environment)
      throws IOException;

  private UUID getExpectedVersion(DataFetchingEnvironment environment) {
    UUID expectedVersion;
    String expectedVersionSpec = environment.getArgument("expectedVersion");
    try {
      expectedVersion = expectedVersionSpec == null ? null : UUID.fromString(expectedVersionSpec);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid 'expectedVersion' value.");
    }
    return expectedVersion;
  }
}
