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
import com.google.common.collect.ImmutableList;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

// TODO purge old entries
public class SchemaSourceDao {

  public static final String TABLE_NAME = "graphql_schema";
  @VisibleForTesting static final String KEY_COLUMN_NAME = "key";
  @VisibleForTesting static final String VERSION_COLUMN_NAME = "version";
  @VisibleForTesting static final String LATEST_VERSION_COLUMN_NAME = "latest_version";
  @VisibleForTesting static final String CONTENTS_COLUMN_NAME = "contents";
  @VisibleForTesting static final String APPLIED_COLUMN_NAME = "[applied]";

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

  public SchemaSource getLatest(String namespace) throws Exception {
    Keyspace keyspace;
    Table table;
    if ((keyspace = dataStore.schema().keyspace(namespace)) == null
        || (table = keyspace.table(TABLE_NAME)) == null) {
      return null;
    }
    failIfUnexpectedSchema(namespace, table);

    Row row = dataStore.execute(schemaQuery(namespace)).get().one();
    if (row == null) {
      return null;
    }
    return toSchemaSource(namespace, row);
  }

  private SchemaSource toSchemaSource(String namespace, Row r) {
    return new SchemaSource(
        namespace, r.getUuid(VERSION_COLUMN_NAME), r.getString(CONTENTS_COLUMN_NAME));
  }

  @VisibleForTesting
  AbstractBound<?> schemaQuery(String namespace) {
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
  public SchemaSource insert(String namespace, UUID expectedLatestVersion, String newContents)
      throws Exception {

    ensureTableExists(namespace);

    UUID newVersion = Uuids.timeBased();

    BoundQuery latestVersionCas =
        dataStore
            .queryBuilder()
            .update(namespace, TABLE_NAME)
            .value(LATEST_VERSION_COLUMN_NAME, newVersion)
            .where(KEY_CONDITION)
            .ifs(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, expectedLatestVersion)
            .build()
            .bind();

    BoundQuery insertNewVersion =
        dataStore
            .queryBuilder()
            .insertInto(namespace, TABLE_NAME)
            .value(KEY_COLUMN_NAME, UNIQUE_KEY)
            .value(VERSION_COLUMN_NAME, newVersion)
            .value(CONTENTS_COLUMN_NAME, newContents)
            .build()
            .bind();

    ResultSet resultSet =
        dataStore.batch(ImmutableList.of(latestVersionCas, insertNewVersion)).get();
    Row row = resultSet.one();
    if (!row.getBoolean(APPLIED_COLUMN_NAME)) {
      UUID actualLatestVersion = row.getUuid(LATEST_VERSION_COLUMN_NAME);
      String message;
      if (expectedLatestVersion == null) {
        message =
            String.format(
                "Could not deploy schema: you indicated your changes were the first version,"
                    + " but there is an existing version '%s'.",
                actualLatestVersion);
      } else {
        message =
            String.format(
                "Could not deploy schema: you indicated your changes applied to version '%s',"
                    + " but there is a more recent version '%s'.",
                expectedLatestVersion, actualLatestVersion);
      }
      throw new IllegalStateException(message);
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
}
