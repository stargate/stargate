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

import static io.stargate.graphql.persistence.schemafirst.SchemaSourceDao.*;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.schema.*;
import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaSourceDaoTest {

  @Test
  public void shouldGetLatestSchema() throws Exception {
    // given
    String namespace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    ResultSet resultSet = mockSchemaResultSet(versionId, schemaContent);
    DataStore dataStore = mockDataStore(namespace, resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    SchemaSource schema = schemaSourceDao.getLatest(namespace);

    // then
    assertThat(schema.getContents()).isEqualTo(schemaContent);
    assertThat(schema.getNamespace()).isEqualTo(namespace);
    assertThat(schema.getVersion()).isEqualTo(versionId);
    assertThat(schema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldReturnNullIfLatestSchemaNotExists() throws Exception {
    // given
    String namespace = "ns_1";
    ResultSet resultSet = mockNullResultSet();
    DataStore dataStore = mockDataStore(namespace, resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    SchemaSource schema = schemaSourceDao.getLatest(namespace);

    // then
    assertThat(schema).isNull();
  }

  private static Stream<Table> notExpectedSchemaProvider() {
    ImmutableTable tableWithoutPartitionKey =
        ImmutableTable.builder()
            .name(TABLE_NAME)
            .keyspace("ns")
            .addColumns(
                ImmutableColumn.create(
                    VERSION_COLUMN_NAME,
                    Column.Kind.Clustering,
                    Column.Type.Timeuuid,
                    Column.Order.DESC))
            .addColumns(
                ImmutableColumn.create(
                    LATEST_VERSION_COLUMN_NAME, Column.Kind.Static, Column.Type.Timeuuid))
            .addColumns(ImmutableColumn.create(CONTENTS_COLUMN_NAME, Column.Type.Varchar))
            .build();

    ImmutableTable tableWithoutClusteringKey =
        ImmutableTable.builder()
            .name(TABLE_NAME)
            .keyspace("ns")
            .addColumns(
                ImmutableColumn.create(
                    KEY_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar))
            .addColumns(
                ImmutableColumn.create(
                    LATEST_VERSION_COLUMN_NAME, Column.Kind.Static, Column.Type.Timeuuid))
            .addColumns(ImmutableColumn.create(CONTENTS_COLUMN_NAME, Column.Type.Varchar))
            .build();

    ImmutableTable tableWithoutLatestVersion =
        ImmutableTable.builder()
            .name(TABLE_NAME)
            .keyspace("ns")
            .addColumns(
                ImmutableColumn.create(
                    KEY_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar))
            .addColumns(
                ImmutableColumn.create(
                    VERSION_COLUMN_NAME,
                    Column.Kind.Clustering,
                    Column.Type.Timeuuid,
                    Column.Order.DESC))
            .addColumns(ImmutableColumn.create(CONTENTS_COLUMN_NAME, Column.Type.Varchar))
            .build();

    ImmutableTable tableWithoutContents =
        ImmutableTable.builder()
            .name(TABLE_NAME)
            .keyspace("ns")
            .addColumns(
                ImmutableColumn.create(
                    KEY_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar))
            .addColumns(
                ImmutableColumn.create(
                    VERSION_COLUMN_NAME,
                    Column.Kind.Clustering,
                    Column.Type.Timeuuid,
                    Column.Order.DESC))
            .addColumns(
                ImmutableColumn.create(
                    LATEST_VERSION_COLUMN_NAME, Column.Kind.Static, Column.Type.Timeuuid))
            .build();
    return Stream.of(
        tableWithoutPartitionKey,
        tableWithoutClusteringKey,
        tableWithoutLatestVersion,
        tableWithoutContents);
  }

  @ParameterizedTest
  @MethodSource("notExpectedSchemaProvider")
  public void shouldFailIfTableDoesNotHaveExpectedSchema(Table table) {
    assertThat(SchemaSourceDao.hasExpectedSchema(table)).isFalse();
  }

  @Test
  public void shouldPassIfTableHasExpectedSchema() {
    // given
    ImmutableTable table =
        ImmutableTable.builder()
            .name(TABLE_NAME)
            .keyspace("ns")
            .addColumns(
                ImmutableColumn.create(
                    KEY_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar))
            .addColumns(
                ImmutableColumn.create(
                    VERSION_COLUMN_NAME,
                    Column.Kind.Clustering,
                    Column.Type.Timeuuid,
                    Column.Order.DESC))
            .addColumns(
                ImmutableColumn.create(
                    LATEST_VERSION_COLUMN_NAME, Column.Kind.Static, Column.Type.Timeuuid))
            .addColumns(ImmutableColumn.create(CONTENTS_COLUMN_NAME, Column.Type.Varchar))
            .build();
    // when, then
    assertThat(SchemaSourceDao.hasExpectedSchema(table)).isTrue();
  }

  @Test
  public void shouldGetSchemaHistory() throws Exception {
    // given
    String namespace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    UUID versionId2 = Uuids.timeBased();
    String schemaContent2 = "some_schema_2";
    ResultSet resultSet =
        mockSchemaResultSetWithTwoRecords(versionId, schemaContent, versionId2, schemaContent2);
    DataStore dataStore = mockDataStore(namespace, resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    List<SchemaSource> schema = schemaSourceDao.getSchemaHistory(namespace);

    // then
    assertThat(schema.size()).isEqualTo(2);
    SchemaSource firstSchema = schema.get(0);
    assertThat(firstSchema.getContents()).isEqualTo(schemaContent);
    assertThat(firstSchema.getNamespace()).isEqualTo(namespace);
    assertThat(firstSchema.getVersion()).isEqualTo(versionId);
    assertThat(firstSchema.getDeployDate()).isNotNull();
    SchemaSource secondSchema = schema.get(1);
    assertThat(secondSchema.getContents()).isEqualTo(schemaContent2);
    assertThat(secondSchema.getNamespace()).isEqualTo(namespace);
    assertThat(secondSchema.getVersion()).isEqualTo(versionId2);
    assertThat(secondSchema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldGetEmptySchemaHistoryIfReturnsNull() throws Exception {
    // given
    String namespace = "ns_1";
    ResultSet resultSet = mockNullResultSet();
    DataStore dataStore = mockDataStore(namespace, resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    List<SchemaSource> schema = schemaSourceDao.getSchemaHistory(namespace);

    // then
    assertThat(schema).isEmpty();
  }

  private DataStore mockDataStore(String namespace, ResultSet resultSet) {
    DataStore dataStore = mock(DataStore.class);
    ImmutableTable table =
        ImmutableTable.builder()
            .name(TABLE_NAME)
            .keyspace(namespace)
            .addColumns(
                ImmutableColumn.create(
                    KEY_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar))
            .addColumns(
                ImmutableColumn.create(
                    VERSION_COLUMN_NAME,
                    Column.Kind.Clustering,
                    Column.Type.Timeuuid,
                    Column.Order.DESC))
            .addColumns(
                ImmutableColumn.create(
                    LATEST_VERSION_COLUMN_NAME, Column.Kind.Static, Column.Type.Timeuuid))
            .addColumns(ImmutableColumn.create(CONTENTS_COLUMN_NAME, Column.Type.Varchar))
            .build();
    Keyspace keyspace = ImmutableKeyspace.builder().addTables(table).name(namespace).build();
    Schema schema = ImmutableSchema.create(Collections.singletonList(keyspace));
    when(dataStore.schema()).thenReturn(schema);
    when(dataStore.execute(any())).thenReturn(CompletableFuture.completedFuture(resultSet));
    return dataStore;
  }

  private ResultSet mockNullResultSet() {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.one()).thenReturn(null);
    return resultSet;
  }

  private ResultSet mockSchemaResultSet(UUID versionId, String schemaContent) {
    Row row = mock(Row.class);
    when(row.getUuid(VERSION_COLUMN_NAME)).thenReturn(versionId);
    when(row.getString(CONTENTS_COLUMN_NAME)).thenReturn(schemaContent);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.one()).thenReturn(row);
    return resultSet;
  }

  private ResultSet mockSchemaResultSetWithTwoRecords(
      UUID versionId, String schemaContent, UUID versionId2, String schemaContent2) {
    Row row = mock(Row.class);
    when(row.getUuid(VERSION_COLUMN_NAME)).thenReturn(versionId);
    when(row.getString(CONTENTS_COLUMN_NAME)).thenReturn(schemaContent);
    Row row2 = mock(Row.class);
    when(row2.getUuid(VERSION_COLUMN_NAME)).thenReturn(versionId2);
    when(row2.getString(CONTENTS_COLUMN_NAME)).thenReturn(schemaContent2);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.rows()).thenReturn(Arrays.asList(row, row2));
    return resultSet;
  }

  static class TestSchemaSourceDao extends SchemaSourceDao {

    public TestSchemaSourceDao(DataStore dataStore) {
      super(dataStore);
    }

    @Override
    AbstractBound<?> schemaQuery(String namespace) {
      return mock(AbstractBound.class);
    }
  }
}
