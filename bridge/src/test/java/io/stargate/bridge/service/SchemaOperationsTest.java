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
package io.stargate.bridge.service;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Static;
import static io.stargate.db.schema.Column.Order.DESC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.bridge.grpc.StargateBearerToken;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import io.stargate.bridge.service.interceptors.NewConnectionInterceptor;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.schema.Column;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

// Note: StargateV2-only test
public class SchemaOperationsTest extends BaseBridgeServiceTest {

  @Captor ArgumentCaptor<Map<String, String>> headers;

  @Test
  @DisplayName(
      "Describe keyspace with a single table with columns using only simple types and no options")
  public void schemaDescribeSingleTableSimpleTypesNoOptions() throws Exception {
    // Given
    StargateBridgeGrpc.StargateBridgeBlockingStub stub =
        makeBlockingStubWithClientHeaders(
                metadata -> {
                  metadata.put(Metadata.Key.of("test-key", ASCII_STRING_MARSHALLER), "test-value");
                })
            .withCallCredentials(new StargateBearerToken("token"));
    when(persistence.decorateKeyspaceName(eq("my_keyspace"), headers.capture()))
        .thenReturn("my_keyspace_decorated");

    io.stargate.db.schema.Schema schema =
        io.stargate.db.schema.Schema.build()
            .keyspace("my_keyspace_decorated")
            .table("my_table")
            .column("key", Column.Type.Text, PartitionKey)
            .column("leaf", Column.Type.Text, Clustering)
            .column("text_value", Column.Type.Text, Static)
            .column("dbl_value", Column.Type.Double)
            .column("bool_value", Column.Type.Boolean)
            .build();

    when(persistence.schema()).thenReturn(schema);
    when(persistence.newConnection(any())).thenReturn(connection);

    AuthenticatedUser authenticatedUser = mock(AuthenticatedUser.class);

    AuthenticationSubject authenticationSubject = mock(AuthenticationSubject.class);
    when(authenticationSubject.asUser()).thenReturn(authenticatedUser);

    AuthenticationService authenticationService = mock(AuthenticationService.class);
    when(authenticationService.validateToken(anyString(), any(Map.class)))
        .thenReturn(authenticationSubject);

    startServer(new NewConnectionInterceptor(persistence, authenticationService));

    // When
    Schema.CqlKeyspaceDescribe response =
        stub.describeKeyspace(
            Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName("my_keyspace").build());

    // Then
    assertThat(headers.getValue()).containsEntry("test-key", "test-value");
    assertThat(response.getCqlKeyspace().getName().equals("my_keyspace")).isTrue();
    assertThat(response.getTablesCount() == 1).isTrue();
    assertThat(response.getTables(0).getName().equals("my_table")).isTrue();
    assertThat(response.getTables(0).getPartitionKeyColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getPartitionKeyColumns(0).getName().equals("key")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getPartitionKeyColumns(0)
                .getType()
                .getBasic()
                .equals(TypeSpec.Basic.VARCHAR))
        .isTrue();
    assertThat(response.getTables(0).getClusteringKeyColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getClusteringKeyColumns(0).getName().equals("leaf")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getClusteringKeyColumns(0)
                .getType()
                .getBasic()
                .equals(TypeSpec.Basic.VARCHAR))
        .isTrue();
    assertThat(response.getTables(0).getClusteringOrdersCount() == 1).isTrue();
    assertThat(
            response
                .getTables(0)
                .getClusteringOrdersMap()
                .get("leaf")
                .equals(Schema.ColumnOrderBy.ASC))
        .isTrue();
    assertThat(response.getTables(0).getStaticColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getStaticColumns(0).getName().equals("text_value")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getStaticColumns(0)
                .getType()
                .getBasic()
                .equals(TypeSpec.Basic.VARCHAR))
        .isTrue();
    assertThat(response.getTables(0).getColumnsCount() == 2).isTrue();
    Map<String, ColumnSpec> columnMap =
        response.getTables(0).getColumnsList().stream()
            .collect(Collectors.toMap(ColumnSpec::getName, Function.identity()));
    assertThat(columnMap.get("dbl_value").getType().getBasic().equals(TypeSpec.Basic.DOUBLE))
        .isTrue();
    assertThat(columnMap.get("bool_value").getType().getBasic().equals(TypeSpec.Basic.BOOLEAN))
        .isTrue();
  }

  @Test
  @DisplayName("Describe table with an index")
  public void schemaDescribeTableWithIndex() {
    // Given
    StargateBridgeGrpc.StargateBridgeBlockingStub stub = makeBlockingStub();
    when(persistence.decorateKeyspaceName(any(String.class), any())).thenReturn("ks");

    io.stargate.db.schema.Schema schema =
        io.stargate.db.schema.Schema.build()
            .keyspace("ks")
            .table("tbl")
            .column("a", Column.Type.Int, PartitionKey)
            .column("b", Column.Type.Text)
            .column("c", Column.Type.Uuid)
            .column("d", Column.Type.Text)
            .secondaryIndex("byB")
            .column("b")
            .build();

    when(persistence.schema()).thenReturn(schema);
    startServer(persistence);

    // When
    Schema.CqlTable response =
        stub.describeKeyspace(
                Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName("ks").build())
            .getTables(0);

    // Then
    assertThat(response.getName().equals("tbl")).isTrue();
    assertThat(response.getPartitionKeyColumnsCount() == 1).isTrue();
    assertThat(response.getPartitionKeyColumns(0).getName().equals("a")).isTrue();
    assertThat(response.getPartitionKeyColumns(0).getType().getBasic().equals(TypeSpec.Basic.INT))
        .isTrue();
    assertThat(response.getColumnsCount() == 3).isTrue();
    Map<String, ColumnSpec> columnMap =
        response.getColumnsList().stream()
            .collect(Collectors.toMap(ColumnSpec::getName, Function.identity()));
    assertThat(columnMap.get("b").getType().getBasic().equals(TypeSpec.Basic.VARCHAR)).isTrue();
    assertThat(columnMap.get("c").getType().getBasic().equals(TypeSpec.Basic.UUID)).isTrue();
    assertThat(columnMap.get("d").getType().getBasic().equals(TypeSpec.Basic.VARCHAR)).isTrue();
    assertThat(response.getIndexesCount() == 1).isTrue();
    assertThat(response.getIndexes(0).getName().equals("byB")).isTrue();
  }

  @Test
  @DisplayName("Describe table with materialized view")
  public void schemaTableWithMaterializedView() {
    // Given
    StargateBridgeGrpc.StargateBridgeBlockingStub stub = makeBlockingStub();
    when(persistence.decorateKeyspaceName(any(String.class), any())).thenReturn("my_stuff");

    io.stargate.db.schema.Schema schema =
        io.stargate.db.schema.Schema.build()
            .keyspace("my_stuff")
            .table("base_table")
            .column("a", Column.Type.Int, PartitionKey)
            .column("b", Column.Type.Text)
            .column("c", Column.Type.Uuid)
            .materializedView("byB")
            .column("b", PartitionKey)
            .column("a", Clustering, DESC)
            .column("c")
            .build();

    when(persistence.schema()).thenReturn(schema);
    startServer(persistence);

    // When
    Schema.CqlTable response =
        stub.describeKeyspace(
                Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName("ks").build())
            .getTables(0);

    // Then
    assertThat(response.getName().equals("base_table")).isTrue();
    assertThat(response.getPartitionKeyColumnsCount() == 1).isTrue();
    assertThat(response.getPartitionKeyColumns(0).getName().equals("a")).isTrue();
    assertThat(response.getPartitionKeyColumns(0).getType().getBasic().equals(TypeSpec.Basic.INT))
        .isTrue();
    assertThat(response.getColumnsCount() == 2).isTrue();
    Map<String, ColumnSpec> columnMap =
        response.getColumnsList().stream()
            .collect(Collectors.toMap(ColumnSpec::getName, Function.identity()));
    assertThat(columnMap.get("b").getType().getBasic().equals(TypeSpec.Basic.VARCHAR)).isTrue();
    assertThat(columnMap.get("c").getType().getBasic().equals(TypeSpec.Basic.UUID)).isTrue();
    assertThat(response.getMaterializedViewsCount() == 1).isTrue();
    assertThat(response.getMaterializedViews(0).getName().equals("byB")).isTrue();
    assertThat(response.getMaterializedViews(0).getPartitionKeyColumnsCount() == 1).isTrue();
    assertThat(response.getMaterializedViews(0).getPartitionKeyColumns(0).getName().equals("b"))
        .isTrue();
    assertThat(response.getMaterializedViews(0).getClusteringKeyColumnsCount() == 1).isTrue();
    assertThat(response.getMaterializedViews(0).getClusteringKeyColumns(0).getName().equals("a"))
        .isTrue();
    assertThat(response.getMaterializedViews(0).getClusteringOrdersCount() == 1).isTrue();
    assertThat(
            response
                .getMaterializedViews(0)
                .getClusteringOrdersMap()
                .get("a")
                .equals(Schema.ColumnOrderBy.DESC))
        .isTrue();
    assertThat(response.getMaterializedViews(0).getColumnsCount() == 1).isTrue();
    assertThat(response.getMaterializedViews(0).getColumns(0).getName().equals("c")).isTrue();
  }

  @Test
  @DisplayName("Handle and propagate server-side NPE appropriately")
  public void schemaTableWithServerSideNPE() {
    // Given
    StargateBridgeGrpc.StargateBridgeBlockingStub stub = makeBlockingStub();
    when(persistence.decorateKeyspaceName(any(String.class), any())).thenReturn("my_stuff");

    when(persistence.schema()).thenThrow(new NullPointerException("No context"));
    startServer(persistence);

    // When
    assertThatThrownBy(
            () ->
                stub.describeKeyspace(
                        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName("ks").build())
                    .getTables(0))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("No context")
        .extracting("status")
        .extracting("code")
        .isEqualTo(Status.UNKNOWN.getCode());
  }
}
