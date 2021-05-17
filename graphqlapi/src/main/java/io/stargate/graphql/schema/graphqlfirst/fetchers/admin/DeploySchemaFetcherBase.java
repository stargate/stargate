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
package io.stargate.graphql.schema.graphqlfirst.fetchers.admin;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.graphqlfirst.migration.CassandraMigrator;
import io.stargate.graphql.schema.graphqlfirst.migration.MigrationQuery;
import io.stargate.graphql.schema.graphqlfirst.migration.MigrationStrategy;
import io.stargate.graphql.schema.graphqlfirst.processor.ProcessedSchema;
import io.stargate.graphql.schema.graphqlfirst.processor.SchemaProcessor;
import io.stargate.graphql.web.HttpAwareContext;
import io.stargate.graphql.web.resources.GraphqlCache;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

abstract class DeploySchemaFetcherBase extends CassandraFetcher<DeploySchemaResponseDto> {

  private final GraphqlCache graphqlCache;

  DeploySchemaFetcherBase(
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory,
      GraphqlCache graphqlCache) {
    super(authorizationService, dataStoreFactory);
    this.graphqlCache = graphqlCache;
  }

  @Override
  protected DeploySchemaResponseDto get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {
    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(dataStore);

    String keyspaceName = environment.getArgument("keyspace");
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      throw new IllegalArgumentException("Keyspace '%s' does not exist.");
    }

    authorizationService.authorizeSchemaWrite(
        authenticationSubject,
        keyspaceName,
        null,
        Scope.MODIFY,
        SourceAPI.GRAPHQL,
        ResourceKind.KEYSPACE);

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
    try {
      Persistence persistence = ((HttpAwareContext) environment.getContext()).getPersistence();
      ProcessedSchema processedSchema =
          new SchemaProcessor(authorizationService, dataStoreFactory, persistence, false)
              .process(input, keyspace);
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
        dataStore.execute(query.build(dataStore)).get();
      }
      SchemaSource newSource = schemaSourceDao.insert(keyspaceName, input);
      schemaSourceDao.purgeOldVersions(keyspaceName);
      response.setVersion(newSource.getVersion());
      graphqlCache.getGraphQLAndUpdateCache(
          keyspaceName, dataStore, newSource, authenticationSubject.customProperties());
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
