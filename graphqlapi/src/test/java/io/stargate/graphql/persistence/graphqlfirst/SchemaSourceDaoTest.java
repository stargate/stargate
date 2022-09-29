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

import static io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao.CONTENTS_COLUMN_NAME;
import static io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao.EXPECTED_TABLE;
import static io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao.KEYSPACE_NAME;
import static io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao.VERSION_COLUMN_NAME;
import static io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao.parseReplication;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.graphqlfirst.util.Uuids;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SchemaSourceDaoTest {

  @Test
  public void shouldGetLatestSchema() throws Exception {
    // given
    String keyspace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    ResultSet resultSet = mockSchemaResultSet(versionId, schemaContent);
    DataStore dataStore = mockDataStore(resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    SchemaSource schema = schemaSourceDao.getLatestVersion(keyspace);

    // then
    assertThat(schema.getContents()).isEqualTo(schemaContent);
    assertThat(schema.getKeyspace()).isEqualTo(keyspace);
    assertThat(schema.getVersion()).isEqualTo(versionId);
    assertThat(schema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldGetSpecificSchema() throws Exception {
    // given
    String keyspace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    ResultSet resultSet = mockSchemaResultSet(versionId, schemaContent);
    DataStore dataStore = mockDataStore(resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    SchemaSource schema = schemaSourceDao.getSingleVersion(keyspace, Optional.of(versionId));

    // then
    assertThat(schema.getContents()).isEqualTo(schemaContent);
    assertThat(schema.getKeyspace()).isEqualTo(keyspace);
    assertThat(schema.getVersion()).isEqualTo(versionId);
    assertThat(schema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldReturnNullIfLatestSchemaNotExists() throws Exception {
    // given
    String keyspace = "ns_1";
    ResultSet resultSet = mockNullResultSet();
    DataStore dataStore = mockDataStore(resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    SchemaSource schema = schemaSourceDao.getLatestVersion(keyspace);

    // then
    assertThat(schema).isNull();
  }

  @Test
  public void shouldGetSchemaHistory() throws Exception {
    // given
    String keyspace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    UUID versionId2 = Uuids.timeBased();
    String schemaContent2 = "some_schema_2";
    ResultSet resultSet =
        mockSchemaResultSetWithTwoRecords(versionId, schemaContent, versionId2, schemaContent2);
    DataStore dataStore = mockDataStore(resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    List<SchemaSource> schema = schemaSourceDao.getAllVersions(keyspace);

    // then
    assertThat(schema.size()).isEqualTo(2);
    SchemaSource firstSchema = schema.get(0);
    assertThat(firstSchema.getContents()).isEqualTo(schemaContent);
    assertThat(firstSchema.getKeyspace()).isEqualTo(keyspace);
    assertThat(firstSchema.getVersion()).isEqualTo(versionId);
    assertThat(firstSchema.getDeployDate()).isNotNull();
    SchemaSource secondSchema = schema.get(1);
    assertThat(secondSchema.getContents()).isEqualTo(schemaContent2);
    assertThat(secondSchema.getKeyspace()).isEqualTo(keyspace);
    assertThat(secondSchema.getVersion()).isEqualTo(versionId2);
    assertThat(secondSchema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldGetEmptySchemaHistoryIfReturnsNull() throws Exception {
    // given
    String keyspace = "ns_1";
    ResultSet resultSet = mockNullResultSet();
    DataStore dataStore = mockDataStore(resultSet);
    SchemaSourceDao schemaSourceDao = new TestSchemaSourceDao(dataStore);

    // when
    List<SchemaSource> schema = schemaSourceDao.getAllVersions(keyspace);

    // then
    assertThat(schema).isEmpty();
  }

  @Test
  @DisplayName("Should default to RF=1 if replication options not provided")
  public void parseNullReplication() {
    Replication replication = parseReplication(null);
    assertThat(replication.toString()).isEqualTo(Replication.simpleStrategy(1).toString());
  }

  @Test
  @DisplayName("Should parse simple replication options")
  public void parseSimpleReplication() {
    Replication replication = parseReplication("2");
    assertThat(replication.toString()).isEqualTo(Replication.simpleStrategy(2).toString());
  }

  @Test
  @DisplayName("Should parse network replication options")
  public void parsNetworkReplication() {
    Replication replication = parseReplication("dc1 = 1, dc2 = 2");
    assertThat(replication.toString())
        .isEqualTo(
            Replication.networkTopologyStrategy(ImmutableMap.of("dc1", 1, "dc2", 2)).toString());
  }

  @Test
  @DisplayName("Should fall back to RF=1 if replication options can't be parsed")
  public void parseInvalidReplication() {
    for (String spec :
        ImmutableList.of(
            // simple, negative RF
            "-1",
            // simple, not a number
            "a",
            // network, invalid RF
            "dc1 = 0, dc2 = 2",
            "dc1 = -1, dc2 = 2",
            // network, not a number
            "dc1 = a, dc2 = 2",
            // network, empty DC
            "= 1, dc2 = 2",
            // network, malformed k/v pair
            "dc1 = 1, dc2")) {
      Replication replication = parseReplication(spec);
      assertThat(replication.toString()).isEqualTo(Replication.simpleStrategy(1).toString());
    }
  }

  private DataStore mockDataStore(ResultSet resultSet) {
    DataStore dataStore = mock(DataStore.class);
    Keyspace keyspace =
        ImmutableKeyspace.builder().addTables(EXPECTED_TABLE).name(KEYSPACE_NAME).build();
    Schema schema = ImmutableSchema.create(Collections.singletonList(keyspace));
    when(dataStore.schema()).thenReturn(schema);
    when(dataStore.execute(any())).thenReturn(CompletableFuture.completedFuture(resultSet));
    return dataStore;
  }

  private ResultSet mockNullResultSet() {
    ResultSet resultSet = mock(ResultSet.class);
    @SuppressWarnings("unchecked")
    Iterator<Row> iterator = mock(Iterator.class);
    when(iterator.hasNext()).thenReturn(false);
    when(resultSet.iterator()).thenReturn(iterator);
    return resultSet;
  }

  private ResultSet mockSchemaResultSet(UUID versionId, String schemaContent) {
    Row row = mock(Row.class);
    when(row.getUuid(VERSION_COLUMN_NAME)).thenReturn(versionId);
    when(row.getString(CONTENTS_COLUMN_NAME)).thenReturn(schemaContent);
    ResultSet resultSet = mock(ResultSet.class);
    @SuppressWarnings("unchecked")
    Iterator<Row> iterator = mock(Iterator.class);
    when(iterator.hasNext()).thenReturn(true);
    when(resultSet.iterator()).thenReturn(iterator);
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
    BoundQuery schemaQuery(String keyspace) {
      return mock(BoundQuery.class);
    }

    @Override
    BoundQuery schemaQueryWithSpecificVersion(String keyspace, UUID uuid) {
      return mock(BoundQuery.class);
    }
  }
}
