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
package io.stargate.graphql.persistence.schemafirst;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

// TODO purge old entries
public class SchemaSourceDao {

  public static final String TABLE_NAME = "graphql_schema";
  @VisibleForTesting static final String KEY_COLUMN_NAME = "key";
  @VisibleForTesting static final String VERSION_COLUMN_NAME = "version";
  @VisibleForTesting static final String LATEST_VERSION_COLUMN_NAME = "latest_version";
  @VisibleForTesting static final String CONTENTS_COLUMN_NAME = "contents";
  @VisibleForTesting static final String APPLIED_COLUMN_NAME = "[applied]";
  private static final String DEPLOYMENT_IN_PROGRESS_COLUMN_NAME = "deployment_in_progress";

  // We use a single partition
  private static final String UNIQUE_KEY = "key";
  static final BuiltCondition KEY_CONDITION =
      BuiltCondition.of(KEY_COLUMN_NAME, Predicate.EQ, UNIQUE_KEY);

  private final DataStore dataStore;

  public SchemaSourceDao(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public List<SchemaSource> getSchemaHistory(String namespace) throws Exception {
    Keyspace keyspace;
    Table table;
    if ((keyspace = dataStore.schema().keyspace(namespace)) == null
        || (table = keyspace.table(TABLE_NAME)) == null) {
      return Collections.emptyList();
    }
    failIfUnexpectedSchema(namespace, table);

    List<Row> row = dataStore.execute(schemaQuery(namespace)).get().rows();
    if (row == null) {
      return Collections.emptyList();
    }
    return row.stream().map(r -> toSchemaSource(namespace, r)).collect(Collectors.toList());
  }

  public SchemaSource getByVersion(
      String namespace,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<UUID> maybeVersion)
      throws Exception {
    Keyspace keyspace;
    Table table;
    if ((keyspace = dataStore.schema().keyspace(namespace)) == null
        || (table = keyspace.table(TABLE_NAME)) == null) {
      return null;
    }
    failIfUnexpectedSchema(namespace, table);

    Row row;

    if (maybeVersion.isPresent()) {
      UUID versionUuid = maybeVersion.get();
      if (versionUuid.version() != 1) { // must be time-based
        return null;
      }
      row = dataStore.execute(schemaQueryWithSpecificVersion(namespace, versionUuid)).get().one();
    } else {
      row = dataStore.execute(schemaQuery(namespace)).get().one();
    }
    if (row == null) {
      return null;
    }
    return toSchemaSource(namespace, row);
  }

  public SchemaSource getLatest(String namespace) throws Exception {
    return getByVersion(namespace, Optional.empty());
  }

  private SchemaSource toSchemaSource(String namespace, Row r) {
    return new SchemaSource(
        namespace, r.getUuid(VERSION_COLUMN_NAME), r.getString(CONTENTS_COLUMN_NAME));
  }

  @VisibleForTesting
  BoundQuery schemaQueryWithSpecificVersion(String namespace, UUID uuid) {
    return dataStore
        .queryBuilder()
        .select()
        .column(VERSION_COLUMN_NAME, CONTENTS_COLUMN_NAME)
        .from(namespace, TABLE_NAME)
        .where(KEY_CONDITION)
        .where(VERSION_COLUMN_NAME, Predicate.EQ, uuid)
        .build()
        .bind();
  }

  @VisibleForTesting
  BoundQuery schemaQuery(String namespace) {
    return dataStore
        .queryBuilder()
        .select()
        .column(VERSION_COLUMN_NAME, CONTENTS_COLUMN_NAME)
        .from(namespace, TABLE_NAME)
        .where(KEY_CONDITION)
        .build()
        .bind();
  }

  /** @return the new version */
  public SchemaSource insert(String namespace, String newContents) throws Exception {

    ensureTableExists(namespace);

    UUID newVersion = Uuids.timeBased();

    BoundQuery insertNewSchema =
        dataStore
            .queryBuilder()
            .insertInto(namespace, TABLE_NAME)
            .value(KEY_COLUMN_NAME, UNIQUE_KEY)
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
              "Schema deployment for namespace: %s and version: %s failed.",
              namespace, newVersion));
    }
    return new SchemaSource(namespace, newVersion, newContents);
  }

  private void ensureTableExists(String namespace) throws Exception {
    dataStore
        .execute(
            dataStore
                .queryBuilder()
                .create()
                .table(namespace, TABLE_NAME)
                .ifNotExists()
                .column(KEY_COLUMN_NAME, Column.Type.Varchar, Column.Kind.PartitionKey)
                .column(
                    VERSION_COLUMN_NAME,
                    Column.Type.Timeuuid,
                    Column.Kind.Clustering,
                    Column.Order.DESC)
                .column(LATEST_VERSION_COLUMN_NAME, Column.Type.Timeuuid, Column.Kind.Static)
                .column(CONTENTS_COLUMN_NAME, Column.Type.Varchar)
                .column(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Column.Type.Boolean, Column.Kind.Static)
                .build()
                .bind())
        .get();

    // If the table already existed, CREATE IF NOT EXISTS does not guarantee that it matches what we
    // were trying to create.
    failIfUnexpectedSchema(namespace, dataStore.schema().keyspace(namespace).table(TABLE_NAME));
  }

  private static void failIfUnexpectedSchema(String namespace, Table table) {
    if (!hasExpectedSchema(table)) {
      throw new IllegalStateException(
          String.format(
              "Table '%s.%s' already exists, but it doesn't have the expected structure",
              namespace, TABLE_NAME));
    }
  }

  @VisibleForTesting
  static boolean hasExpectedSchema(Table table) {
    List<Column> partitionKeyColumns = table.partitionKeyColumns();
    if (partitionKeyColumns.size() != 1) {
      return false;
    }
    Column key = partitionKeyColumns.get(0);
    if (!KEY_COLUMN_NAME.equals(key.name()) || key.type() != Column.Type.Varchar) {
      return false;
    }
    List<Column> clusteringKeyColumns = table.clusteringKeyColumns();
    if (clusteringKeyColumns.size() != 1) {
      return false;
    }
    Column version = clusteringKeyColumns.get(0);
    if (!VERSION_COLUMN_NAME.equals(version.name())
        || version.type() != Column.Type.Timeuuid
        || version.order() != Column.Order.DESC) {
      return false;
    }
    Column latestVersion = table.column(LATEST_VERSION_COLUMN_NAME);
    if (latestVersion == null
        || !LATEST_VERSION_COLUMN_NAME.equals(latestVersion.name())
        || latestVersion.type() != Column.Type.Timeuuid
        || latestVersion.kind() != Column.Kind.Static) {
      return false;
    }
    Column contents = table.column(CONTENTS_COLUMN_NAME);
    return contents != null && contents.type() == Column.Type.Varchar;
  }

  /**
   * Returns true if there was no schema deployment in progress. It means that that is safe to
   * deploy a new schema. If it returns false, it means that there is already schema deployment in
   * progress.
   */
  public boolean startDeployment(String namespace, UUID expectedLatestVersion) throws Exception {
    ensureTableExists(namespace);
    BoundQuery updateDeploymentToInProgress =
        dataStore
            .queryBuilder()
            .update(namespace, TABLE_NAME)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, true)
            .where(KEY_CONDITION)
            .ifs(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Predicate.NEQ, true)
            .ifs(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, expectedLatestVersion)
            .build()
            .bind();

    ResultSet resultSet = dataStore.execute(updateDeploymentToInProgress).get();
    Row row = resultSet.one();
    return row.getBoolean(APPLIED_COLUMN_NAME);
  }

  public void abortDeployment(String namespace) throws ExecutionException, InterruptedException {
    BoundQuery updateDeploymentToNotInProgress =
        dataStore
            .queryBuilder()
            .update(namespace, TABLE_NAME)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, false)
            .where(KEY_CONDITION)
            .build()
            .bind();
    dataStore.execute(updateDeploymentToNotInProgress).get();
  }
}
