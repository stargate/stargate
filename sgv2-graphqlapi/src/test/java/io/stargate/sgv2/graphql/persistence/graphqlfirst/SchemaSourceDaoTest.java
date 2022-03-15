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
package io.stargate.sgv2.graphql.persistence.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Basic;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.schema.Uuids;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaSourceDaoTest {

  @Test
  public void shouldGetLatestSchema() {
    // given
    String keyspace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    ResultSet resultSet = mockOneVersion(versionId, schemaContent);
    StargateBridgeClient bridge = mockBridge(resultSet);
    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(bridge);

    // when
    SchemaSource schema =
        Futures.getUninterruptibly(schemaSourceDao.getLatestVersionAsync(keyspace))
            .orElseThrow(AssertionError::new);

    // then
    assertThat(schema.getContents()).isEqualTo(schemaContent);
    assertThat(schema.getKeyspace()).isEqualTo(keyspace);
    assertThat(schema.getVersion()).isEqualTo(versionId);
    assertThat(schema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldGetSpecificSchema() {
    // given
    String keyspace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    ResultSet resultSet = mockOneVersion(versionId, schemaContent);
    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(mockBridge(resultSet));

    // when
    SchemaSource schema =
        schemaSourceDao
            .getSingleVersion(keyspace, Optional.of(versionId))
            .orElseThrow(AssertionError::new);

    // then
    assertThat(schema.getContents()).isEqualTo(schemaContent);
    assertThat(schema.getKeyspace()).isEqualTo(keyspace);
    assertThat(schema.getVersion()).isEqualTo(versionId);
    assertThat(schema.getDeployDate()).isNotNull();
  }

  @Test
  public void shouldReturnNullIfLatestSchemaNotExists() {
    // given
    String keyspace = "ns_1";
    ResultSet resultSet = mockNoVersions();
    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(mockBridge(resultSet));

    // when
    SchemaSource schema =
        Futures.getUninterruptibly(schemaSourceDao.getLatestVersionAsync(keyspace))
            .orElseThrow(AssertionError::new);

    // then
    assertThat(schema).isNull();
  }

  @Test
  public void shouldGetSchemaHistory() {
    // given
    String keyspace = "ns_1";
    UUID versionId = Uuids.timeBased();
    String schemaContent = "some_schema";
    UUID versionId2 = Uuids.timeBased();
    String schemaContent2 = "some_schema_2";
    ResultSet resultSet = mockTwoVersions(versionId, schemaContent, versionId2, schemaContent2);
    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(mockBridge(resultSet));

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
  public void shouldGetEmptySchemaHistoryIfReturnsNull() {
    // given
    String keyspace = "ns_1";
    ResultSet resultSet = mockNoVersions();
    SchemaSourceDao schemaSourceDao = new SchemaSourceDao(mockBridge(resultSet));

    // when
    List<SchemaSource> schema = schemaSourceDao.getAllVersions(keyspace);

    // then
    assertThat(schema).isEmpty();
  }

  @Test
  @DisplayName("Should default to RF=1 if replication options not provided")
  public void parseNullReplication() {
    Replication replication = SchemaSourceDao.parseReplication(null);
    assertThat(replication.toString()).isEqualTo(Replication.simpleStrategy(1).toString());
  }

  @Test
  @DisplayName("Should parse simple replication options")
  public void parseSimpleReplication() {
    Replication replication = SchemaSourceDao.parseReplication("2");
    assertThat(replication.toString()).isEqualTo(Replication.simpleStrategy(2).toString());
  }

  @Test
  @DisplayName("Should parse network replication options")
  public void parsNetworkReplication() {
    Replication replication = SchemaSourceDao.parseReplication("dc1 = 1, dc2 = 2");
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
      Replication replication = SchemaSourceDao.parseReplication(spec);
      assertThat(replication.toString()).isEqualTo(Replication.simpleStrategy(1).toString());
    }
  }

  private StargateBridgeClient mockBridge(ResultSet resultSet) {
    StargateBridgeClient bridge = mock(StargateBridgeClient.class);

    // No need to mock the actual table structure, this is only needed for an existence check
    Optional<CqlTable> table = Optional.of(CqlTable.newBuilder().build());
    when(bridge.getTableAsync(SchemaSourceDao.KEYSPACE_NAME, SchemaSourceDao.TABLE_NAME, false))
        .thenReturn(CompletableFuture.completedFuture(table));

    when(bridge.executeQuery(any()))
        .thenReturn(Response.newBuilder().setResultSet(resultSet).build());
    when(bridge.executeQueryAsync(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Response.newBuilder().setResultSet(resultSet).build()));

    return bridge;
  }

  private ResultSet mockNoVersions() {
    return mockVersions();
  }

  private ResultSet mockOneVersion(UUID versionId, String schemaContent) {
    return mockVersions(
        Row.newBuilder()
            .addValues(Values.of(versionId))
            .addValues(Values.of(schemaContent))
            .build());
  }

  private ResultSet mockTwoVersions(
      UUID versionId, String schemaContent, UUID versionId2, String schemaContent2) {
    return mockVersions(
        Row.newBuilder()
            .addValues(Values.of(versionId))
            .addValues(Values.of(schemaContent))
            .build(),
        Row.newBuilder()
            .addValues(Values.of(versionId2))
            .addValues(Values.of(schemaContent2))
            .build());
  }

  private ResultSet mockVersions(Row... rows) {
    return ResultSet.newBuilder()
        .addColumns(
            ColumnSpec.newBuilder()
                .setName(SchemaSourceDao.VERSION_COLUMN_NAME)
                .setType(TypeSpec.newBuilder().setBasic(Basic.TIMEUUID)))
        .addColumns(
            ColumnSpec.newBuilder()
                .setName(SchemaSourceDao.CONTENTS_COLUMN_NAME)
                .setType(TypeSpec.newBuilder().setBasic(Basic.VARCHAR)))
        .addAllRows(Arrays.asList(rows))
        .build();
  }
}
