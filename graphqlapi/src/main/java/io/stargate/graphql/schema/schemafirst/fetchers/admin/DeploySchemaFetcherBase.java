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
package io.stargate.graphql.schema.schemafirst.fetchers.admin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.GraphqlErrorHelper;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.schemafirst.migration.CassandraMigrator;
import io.stargate.graphql.schema.schemafirst.migration.MigrationQuery;
import io.stargate.graphql.schema.schemafirst.migration.MigrationStrategy;
import io.stargate.graphql.schema.schemafirst.processor.ProcessedSchema;
import io.stargate.graphql.schema.schemafirst.processor.ProcessingMessage;
import io.stargate.graphql.schema.schemafirst.processor.SchemaProcessor;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

abstract class DeploySchemaFetcherBase extends CassandraFetcher<Map<String, Object>> {

  DeploySchemaFetcherBase(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Map<String, Object> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception {

    String namespace = environment.getArgument("namespace");
    Keyspace keyspace = dataStore.schema().keyspace(namespace);
    if (keyspace == null) {
      throw new IllegalArgumentException(
          String.format(
              "Namespace '%s' does not exist. "
                  + "Use the 'createNamespace' mutation to create it first.",
              namespace));
    }

    authorizationService.authorizeSchemaWrite(
        authenticationSubject, namespace, null, Scope.MODIFY, SourceAPI.GRAPHQL);

    String input = getSchemaContents(environment);
    UUID expectedVersion = getExpectedVersion(environment);
    MigrationStrategy migrationStrategy = environment.getArgument("migrationStrategy");
    boolean dryRun = environment.getArgument("dryRun");

    ImmutableMap.Builder<String, Object> response = ImmutableMap.builder();
    ProcessedSchema processedSchema =
        new SchemaProcessor(authenticationService, authorizationService, dataStoreFactory)
            .process(input, keyspace);
    response.put(
        "messages",
        processedSchema.getMessages().stream()
            .map(this::formatMessage)
            .collect(Collectors.toList()));

    // TODO the rest of the method is racy -- we should at least ensure that two deploy mutations
    // can't compete with each other (maybe add a "migration in progress" column in the database and
    // do an additional LWT on it)
    List<MigrationQuery> queries =
        new CassandraMigrator(dataStore, processedSchema.getMappingModel(), migrationStrategy)
            .compute();

    response.put(
        "cqlChanges",
        queries.isEmpty()
            ? ImmutableList.of("No changes, the CQL schema is up to date")
            : queries.stream().map(MigrationQuery::getDescription).collect(Collectors.toList()));

    if (!dryRun) {
      for (MigrationQuery query : queries) {
        dataStore.execute(query.build(dataStore)).get();
      }
      SchemaSource newSource =
          new SchemaSourceDao(dataStore).insert(namespace, expectedVersion, input);
      response.put("version", newSource.getVersion());
      // TODO update SchemaFirstCache from here instead of letting it reload from the DB
    }

    return response.build();
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

  private Map<String, Object> formatMessage(ProcessingMessage message) {
    return ImmutableMap.of(
        "contents",
        message.getMessage(),
        "category",
        message.getErrorType().toString().toUpperCase(Locale.ENGLISH),
        "locations",
        message.getLocations().stream()
            .map(GraphqlErrorHelper::location)
            .collect(Collectors.toList()));
  }
}
