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
package io.stargate.graphql.persistence.graphqlfirst;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.graphqlfirst.migration.CassandraSchemaHelper;
import io.stargate.graphql.schema.graphqlfirst.util.Uuids;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSourceDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSourceDao.class);
  public static final String KEYSPACE_NAME = "stargate_graphql";
  public static final String TABLE_NAME = "schema_source";
  public static final String KEYSPACE_REPLICATION_PROPERTY =
      "stargate.graphql_first.replication_options";
  private static final Replication DEFAULT_KEYSPACE_REPLICATION = Replication.simpleStrategy(1);
  private static final Replication KEYSPACE_REPLICATION =
      parseReplication(System.getProperty(KEYSPACE_REPLICATION_PROPERTY));

  @VisibleForTesting static final String KEYSPACE_COLUMN_NAME = "keyspace_name";
  @VisibleForTesting static final String VERSION_COLUMN_NAME = "version";
  @VisibleForTesting static final String LATEST_VERSION_COLUMN_NAME = "latest_version";
  @VisibleForTesting static final String CONTENTS_COLUMN_NAME = "contents";
  @VisibleForTesting static final String APPLIED_COLUMN_NAME = "[applied]";

  @VisibleForTesting
  static final String DEPLOYMENT_IN_PROGRESS_COLUMN_NAME = "deployment_in_progress";

  private static final int NUMBER_OF_RETAINED_SCHEMA_VERSIONS = 10;

  @VisibleForTesting
  static final Table EXPECTED_TABLE =
      ImmutableTable.builder()
          .keyspace(KEYSPACE_NAME)
          .name(TABLE_NAME)
          .addColumns(
              ImmutableColumn.create(
                  KEYSPACE_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar),
              ImmutableColumn.create(
                  VERSION_COLUMN_NAME,
                  Column.Kind.Clustering,
                  Column.Type.Timeuuid,
                  Column.Order.DESC),
              ImmutableColumn.create(
                  CONTENTS_COLUMN_NAME, Column.Kind.Regular, Column.Type.Varchar),
              ImmutableColumn.create(
                  LATEST_VERSION_COLUMN_NAME, Column.Kind.Static, Column.Type.Timeuuid),
              ImmutableColumn.create(
                  DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Column.Kind.Static, Column.Type.Boolean))
          .build();

  private final DataStore dataStore;

  public SchemaSourceDao(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public List<SchemaSource> getAllVersions(String keyspace) throws Exception {
    if (!tableExists()) {
      return Collections.emptyList();
    }
    List<Row> row = dataStore.execute(schemaQuery(keyspace)).get().rows();
    if (row == null) {
      return Collections.emptyList();
    }
    return row.stream().map(r -> toSchemaSource(keyspace, r)).collect(Collectors.toList());
  }

  public SchemaSource getSingleVersion(
      String keyspace,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<UUID> maybeVersion)
      throws Exception {
    if (!tableExists()) {
      return null;
    }
    ResultSet resultSet;
    if (maybeVersion.isPresent()) {
      UUID versionUuid = maybeVersion.get();
      if (versionUuid.version() != 1) { // must be time-based
        return null;
      }
      resultSet = dataStore.execute(schemaQueryWithSpecificVersion(keyspace, versionUuid)).get();
    } else {
      resultSet = dataStore.execute(schemaQuery(keyspace)).get();
    }
    if (!resultSet.iterator().hasNext()) {
      return null;
    }
    return toSchemaSource(keyspace, resultSet.one());
  }

  public SchemaSource getLatestVersion(String keyspace) throws Exception {
    return getSingleVersion(keyspace, Optional.empty());
  }

  private SchemaSource toSchemaSource(String keyspace, Row r) {
    return new SchemaSource(
        keyspace, r.getUuid(VERSION_COLUMN_NAME), r.getString(CONTENTS_COLUMN_NAME));
  }

  @VisibleForTesting
  BoundQuery schemaQueryWithSpecificVersion(String keyspace, UUID uuid) {
    return dataStore
        .queryBuilder()
        .select()
        .column(VERSION_COLUMN_NAME, CONTENTS_COLUMN_NAME)
        .from(KEYSPACE_NAME, TABLE_NAME)
        .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, keyspace)
        .where(VERSION_COLUMN_NAME, Predicate.EQ, uuid)
        .build()
        .bind();
  }

  @VisibleForTesting
  BoundQuery schemaQuery(String keyspace) {
    return dataStore
        .queryBuilder()
        .select()
        .column(VERSION_COLUMN_NAME, CONTENTS_COLUMN_NAME)
        .from(KEYSPACE_NAME, TABLE_NAME)
        .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, keyspace)
        .orderBy(VERSION_COLUMN_NAME, Column.Order.DESC)
        .build()
        .bind();
  }

  /** @return the new version */
  public SchemaSource insert(String keyspace, String newContents) {

    UUID newVersion = Uuids.timeBased();

    BoundQuery insertNewSchema =
        dataStore
            .queryBuilder()
            .insertInto(KEYSPACE_NAME, TABLE_NAME)
            .value(KEYSPACE_COLUMN_NAME, keyspace)
            .value(VERSION_COLUMN_NAME, newVersion)
            .value(LATEST_VERSION_COLUMN_NAME, newVersion)
            .value(CONTENTS_COLUMN_NAME, newContents)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, false)
            .build()
            .bind();

    try {
      dataStore.execute(insertNewSchema).get();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Schema deployment for keyspace: %s and version: %s failed.", keyspace, newVersion));
    }
    return new SchemaSource(keyspace, newVersion, newContents);
  }

  private void ensureTableExists() throws Exception {
    if (tableExists()) {
      return;
    }
    dataStore
        .execute(
            dataStore
                .queryBuilder()
                .create()
                .keyspace(KEYSPACE_NAME)
                .ifNotExists()
                .withReplication(KEYSPACE_REPLICATION)
                .build()
                .bind())
        .get();
    dataStore
        .execute(
            dataStore
                .queryBuilder()
                .create()
                .table(KEYSPACE_NAME, TABLE_NAME)
                .ifNotExists()
                .column(EXPECTED_TABLE.columns())
                .build()
                .bind())
        .get();

    // Just in case our `CREATE IF NOT EXISTS` calls raced with another client:
    failIfUnexpectedSchema(dataStore.schema().keyspace(KEYSPACE_NAME).table(TABLE_NAME));
  }

  private boolean tableExists() {
    Keyspace keyspace = dataStore.schema().keyspace(KEYSPACE_NAME);
    if (keyspace == null) {
      return false;
    }
    Table table = keyspace.table(TABLE_NAME);
    if (table == null) {
      return false;
    }
    failIfUnexpectedSchema(table);
    return true;
  }

  private static void failIfUnexpectedSchema(Table table) {
    if (!CassandraSchemaHelper.compare(EXPECTED_TABLE, table).isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Table '%s.%s' already exists, but it doesn't have the expected structure",
              KEYSPACE_NAME, TABLE_NAME));
    }
  }

  /**
   * "Locks" the table to start a new deployment. Concurrent calls to this method will fail until
   * either {@link #abortDeployment(String)} or {@link #insert(String, String)} have been called.
   *
   * @throws IllegalStateException if the deployment could not be started.
   */
  public void startDeployment(String keyspace, UUID expectedLatestVersion, boolean force)
      throws Exception {
    ensureTableExists();
    List<BuiltCondition> conditions =
        force
            ? ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, expectedLatestVersion))
            : ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, expectedLatestVersion),
                BuiltCondition.of(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Predicate.NEQ, true));
    BoundQuery updateDeploymentToInProgress =
        dataStore
            .queryBuilder()
            .update(KEYSPACE_NAME, TABLE_NAME)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, true)
            .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, keyspace)
            .ifs(conditions)
            .build()
            .bind();

    ResultSet resultSet = dataStore.execute(updateDeploymentToInProgress).get();
    Row row = resultSet.one();
    if (!row.getBoolean(APPLIED_COLUMN_NAME)) {
      handleFailedDeployLwt(row, expectedLatestVersion);
    }
  }

  private void handleFailedDeployLwt(Row row, UUID expectedLatestVersion) {
    boolean hasVersion =
        row.columns().stream().anyMatch(c -> LATEST_VERSION_COLUMN_NAME.equals(c.name()));
    if (!hasVersion) {
      throw new IllegalStateException(
          "You specified expectedVersion but no previous version was found");
    }
    UUID actualLatestVersion = row.getUuid(LATEST_VERSION_COLUMN_NAME);
    if (Objects.equals(actualLatestVersion, expectedLatestVersion)) {
      assert row.getBoolean(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME);
      throw new IllegalStateException(
          "It looks like someone else is deploying a new schema, please check the latest version and try again. "
              + "This can also happen if a previous deployment failed unexpectedly, in that case you can use the "
              + "'force' argument to bypass this check.");
    }
    throw new IllegalStateException(
        String.format(
            "You specified expectedVersion %s, but there is a more recent version %s",
            expectedLatestVersion, actualLatestVersion));
  }

  public void abortDeployment(String keyspace) throws ExecutionException, InterruptedException {
    BoundQuery updateDeploymentToNotInProgress =
        dataStore
            .queryBuilder()
            .update(KEYSPACE_NAME, TABLE_NAME)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, false)
            .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, keyspace)
            .build()
            .bind();
    dataStore.execute(updateDeploymentToNotInProgress).get();
  }

  public void undeploy(String keyspace, UUID expectedLatestVersion, boolean force)
      throws ExecutionException, InterruptedException {
    List<BuiltCondition> conditions =
        force
            ? ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, expectedLatestVersion))
            : ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, expectedLatestVersion),
                BuiltCondition.of(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Predicate.NEQ, true));
    BoundQuery clearLatestVersion =
        dataStore
            .queryBuilder()
            .update(KEYSPACE_NAME, TABLE_NAME)
            .value(LATEST_VERSION_COLUMN_NAME, null)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, false)
            .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, keyspace)
            .ifs(conditions)
            .build()
            .bind();
    Row row = dataStore.execute(clearLatestVersion).get().one();
    if (!row.getBoolean(APPLIED_COLUMN_NAME)) {
      handleFailedDeployLwt(row, expectedLatestVersion);
    }
  }

  public void purgeOldVersions(String keyspace) throws Exception {
    List<SchemaSource> allSchemasForKeyspace = getAllVersions(keyspace);

    int numberOfEntriesToRemove = allSchemasForKeyspace.size() - NUMBER_OF_RETAINED_SCHEMA_VERSIONS;
    if (numberOfEntriesToRemove > 0) {
      LOGGER.info("Removing {} old schema entries.", numberOfEntriesToRemove);

      // remove N oldest entries
      SchemaSource mostRecentToRemove =
          allSchemasForKeyspace.get(NUMBER_OF_RETAINED_SCHEMA_VERSIONS);

      BoundQuery deleteSchemaQuery =
          dataStore
              .queryBuilder()
              .delete()
              .from(KEYSPACE_NAME, TABLE_NAME)
              .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, keyspace)
              .where(VERSION_COLUMN_NAME, Predicate.LTE, mostRecentToRemove.getVersion())
              .build()
              .bind();
      dataStore.execute(deleteSchemaQuery).get();
    }
  }

  @VisibleForTesting
  static Replication parseReplication(String spec) {
    if (spec == null) {
      LOGGER.debug("No replication configured, defaulting to {}", DEFAULT_KEYSPACE_REPLICATION);
      return DEFAULT_KEYSPACE_REPLICATION;
    }

    try {
      Replication replication =
          spec.matches("\\d+") ? parseSimpleReplication(spec) : parseNetworkReplication(spec);
      LOGGER.debug("Using configured replication {}", replication);
      return replication;
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "Could not parse replication '{}' (from {}). Falling back to default {}",
          spec,
          KEYSPACE_REPLICATION_PROPERTY,
          DEFAULT_KEYSPACE_REPLICATION);
      return DEFAULT_KEYSPACE_REPLICATION;
    }
  }

  private static Replication parseSimpleReplication(String spec) {
    int rf = Integer.parseInt(spec);
    if (rf < 1) {
      throw new IllegalArgumentException();
    }
    return Replication.simpleStrategy(rf);
  }

  private static Replication parseNetworkReplication(String spec) {
    Map<String, String> rawOptions =
        Splitter.on(",").withKeyValueSeparator(Splitter.on("=").trimResults()).split(spec);
    Map<String, Integer> options = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : rawOptions.entrySet()) {
      String dc = entry.getKey();
      if (dc.isEmpty()) {
        throw new IllegalArgumentException();
      }
      int rf = Integer.parseInt(entry.getValue());
      if (rf < 1) {
        throw new IllegalArgumentException();
      }
      options.put(dc, rf);
    }
    return Replication.networkTopologyStrategy(options);
  }
}
