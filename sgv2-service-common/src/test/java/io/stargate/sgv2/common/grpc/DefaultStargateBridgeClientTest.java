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
package io.stargate.sgv2.common.grpc;

import static io.stargate.sgv2.common.grpc.DefaultStargateBridgeClient.SELECT_KEYSPACE_NAMES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.Schema.AuthorizeSchemaReadsRequest;
import io.stargate.proto.Schema.AuthorizeSchemaReadsResponse;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.SchemaRead;
import io.stargate.proto.Schema.SchemaRead.SourceApi;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeImplBase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultStargateBridgeClientTest {

  private static final String SERVER_NAME = "MockBridge";
  private static final String AUTH_TOKEN = "MockAuthToken";
  private static final SourceApi SOURCE_API = SourceApi.REST;

  private Server server;
  private ManagedChannel channel;
  @Mock private StargateBridgeImplBase service;
  @Mock private DefaultStargateBridgeSchema schema;

  @BeforeEach
  public void setup() throws IOException {
    server =
        InProcessServerBuilder.forName(SERVER_NAME).directExecutor().addService(service).build();
    server.start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
  }

  @AfterEach
  public void teardown() {
    server.shutdownNow();
  }

  @Test
  public void executeQuery() {
    // Given
    Query query = Query.newBuilder().setCql("mock CQL query").build();
    List<Row> rows =
        ImmutableList.of(
            Row.newBuilder().addValues(Values.of("a")).build(),
            Row.newBuilder().addValues(Values.of("b")).build());
    mockQuery(query, rows);

    // When
    Response response = newClient().executeQuery(query);

    // Then
    assertThat(response.getResultSet().getRowsList()).isEqualTo(rows);
  }

  @Test
  public void executeBatch() {
    // Given
    Batch batch =
        Batch.newBuilder()
            .addQueries(BatchQuery.newBuilder().setCql("mock CQL query 1"))
            .addQueries(BatchQuery.newBuilder().setCql("mock CQL query 2"))
            .build();
    List<Row> rows =
        ImmutableList.of(
            Row.newBuilder().addValues(Values.of("a")).build(),
            Row.newBuilder().addValues(Values.of("b")).build());
    mockBatch(batch, rows);

    // When
    Response response = newClient().executeBatch(batch);

    // Then
    assertThat(response.getResultSet().getRowsList()).isEqualTo(rows);
  }

  @Test
  public void getKeyspace() {
    // Given
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    CqlKeyspaceDescribe schemaKeyspace = mockKeyspace(keyspaceName);
    when(schema.getKeyspaceAsync(keyspaceName))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace));

    // When
    Optional<CqlKeyspaceDescribe> keyspace = newClient().getKeyspace(keyspaceName);

    // Then
    assertThat(keyspace).hasValue(schemaKeyspace);
  }

  @Test
  public void getKeyspaceNonExistent() {
    // Given
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    when(schema.getKeyspaceAsync(keyspaceName)).thenReturn(CompletableFuture.completedFuture(null));

    // When
    Optional<CqlKeyspaceDescribe> keyspace = newClient().getKeyspace(keyspaceName);

    // Then
    assertThat(keyspace).isEmpty();
  }

  @Test
  public void getKeyspaceUnauthorized() {
    // Given
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), false);

    // Then
    assertThatThrownBy(() -> newClient().getKeyspace(keyspaceName))
        .isInstanceOf(UnauthorizedKeyspaceException.class);
  }

  @Test
  public void getKeyspaceWithTenantId() {
    // Given
    String tenantId = "tenant1";
    String keyspaceName = "ks";
    String decoratedKeyspaceName =
        Hex.encodeHexString(tenantId.getBytes(StandardCharsets.UTF_8)) + '_' + keyspaceName;
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    CqlKeyspaceDescribe schemaKeyspace = mockKeyspace(decoratedKeyspaceName);
    when(schema.getKeyspaceAsync(decoratedKeyspaceName))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace));

    // When
    Optional<CqlKeyspaceDescribe> keyspace = newClient(tenantId).getKeyspace(keyspaceName);

    // Then
    assertThat(keyspace)
        .hasValueSatisfying(d -> assertThat(d.getCqlKeyspace().getName()).isEqualTo(keyspaceName));
  }

  @Test
  public void getKeyspaces() {
    // Given
    mockKeyspaceNames("ks1", "ks2", "ks3");
    mockAuthorizations(
        ImmutableMap.of(
            SchemaReads.keyspace("ks1", SOURCE_API),
            true,
            SchemaReads.keyspace("ks2", SOURCE_API),
            false,
            SchemaReads.keyspace("ks3", SOURCE_API),
            true));
    CqlKeyspaceDescribe schemaKeyspace1 = mockKeyspace("ks1");
    when(schema.getKeyspaceAsync("ks1"))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace1));
    CqlKeyspaceDescribe schemaKeyspace3 = mockKeyspace("ks3");
    when(schema.getKeyspaceAsync("ks3"))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace3));

    // When
    List<CqlKeyspaceDescribe> keyspaces = newClient().getAllKeyspaces();

    // Then
    assertThat(keyspaces).extracting(k -> k.getCqlKeyspace().getName()).contains("ks1", "ks3");
  }

  @Test
  public void getTable() {
    // Given
    String keyspaceName = "ks";
    String tableName = "tbl";
    mockAuthorization(SchemaReads.table(keyspaceName, tableName, SOURCE_API), true);
    CqlKeyspaceDescribe schemaKeyspace = mockKeyspace(keyspaceName, tableName);
    when(schema.getKeyspaceAsync(keyspaceName))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace));

    // When
    Optional<CqlTable> table = newClient().getTable(keyspaceName, tableName);

    // Then
    assertThat(table).hasValueSatisfying(t -> assertThat(t.getName()).isEqualTo(tableName));
  }

  @Test
  public void getTableNonExistent() {
    // Given
    String keyspaceName = "ks";
    String tableName = "tbl";
    mockAuthorization(SchemaReads.table(keyspaceName, tableName, SOURCE_API), true);
    CqlKeyspaceDescribe schemaKeyspace = mockKeyspace(keyspaceName);
    when(schema.getKeyspaceAsync(keyspaceName))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace));

    // When
    Optional<CqlTable> table = newClient().getTable(keyspaceName, tableName);

    // Then
    assertThat(table).isEmpty();
  }

  @Test
  public void getTableUnauthorized() {
    // Given
    String keyspaceName = "ks";
    String tableName = "tbl";
    mockAuthorization(SchemaReads.table(keyspaceName, tableName, SOURCE_API), false);

    // Then
    assertThatThrownBy(() -> newClient().getTable(keyspaceName, tableName))
        .isInstanceOf(UnauthorizedTableException.class);
  }

  @Test
  public void getTables() {
    // Given
    String keyspaceName = "ks";
    mockAuthorizations(
        ImmutableMap.of(
            SchemaReads.table(keyspaceName, "tbl1", SOURCE_API), true,
            SchemaReads.table(keyspaceName, "tbl2", SOURCE_API), false,
            SchemaReads.table(keyspaceName, "tbl3", SOURCE_API), true));

    CqlKeyspaceDescribe schemaKeyspace = mockKeyspace(keyspaceName, "tbl1", "tbl2", "tbl3");
    when(schema.getKeyspaceAsync(keyspaceName))
        .thenReturn(CompletableFuture.completedFuture(schemaKeyspace));

    // When
    List<CqlTable> tables = newClient().getTables(keyspaceName);

    // Then
    assertThat(tables).extracting(CqlTable::getName).contains("tbl1", "tbl3");
  }

  private CqlKeyspaceDescribe mockKeyspace(String keyspaceName, String... tableNames) {
    return CqlKeyspaceDescribe.newBuilder()
        .setCqlKeyspace(CqlKeyspace.newBuilder().setName(keyspaceName))
        .addAllTables(
            Arrays.stream(tableNames)
                .map(n -> CqlTable.newBuilder().setName(n).build())
                .collect(Collectors.toList()))
        .build();
  }

  private DefaultStargateBridgeClient newClient() {
    return new DefaultStargateBridgeClient(
        channel, schema, AUTH_TOKEN, Optional.empty(), SOURCE_API);
  }

  private DefaultStargateBridgeClient newClient(String tenantId) {
    return new DefaultStargateBridgeClient(
        channel, schema, AUTH_TOKEN, Optional.of(tenantId), SOURCE_API);
  }

  void mockAuthorizations(Map<SchemaRead, Boolean> authorizations) {
    doAnswer(
            i -> {
              AuthorizeSchemaReadsRequest request = i.getArgument(0);
              StreamObserver<AuthorizeSchemaReadsResponse> observer = i.getArgument(1);
              List<Boolean> authorizeds = new ArrayList<>();
              for (SchemaRead read : request.getSchemaReadsList()) {
                Boolean authorized = authorizations.get(read);
                assertThat(authorized).isNotNull();
                authorizeds.add(authorized);
              }
              observer.onNext(
                  AuthorizeSchemaReadsResponse.newBuilder().addAllAuthorized(authorizeds).build());
              observer.onCompleted();
              return null;
            })
        .when(service)
        .authorizeSchemaReads(any(), any());
  }

  void mockAuthorization(SchemaRead read, boolean authorized) {
    mockAuthorizations(ImmutableMap.of(read, authorized));
  }

  void mockKeyspaceNames(String... keyspaceNames) {
    mockQuery(
        SELECT_KEYSPACE_NAMES,
        Arrays.stream(keyspaceNames)
            .map(n -> Row.newBuilder().addValues(Values.of(n)).build())
            .collect(Collectors.toList()));
  }

  private void mockQuery(Query query, List<Row> rows) {
    doAnswer(i -> mockResponse(i, rows)).when(service).executeQuery(Mockito.eq(query), any());
  }

  private void mockBatch(Batch batch, List<Row> rows) {
    doAnswer(i -> mockResponse(i, rows)).when(service).executeBatch(Mockito.eq(batch), any());
  }

  private Void mockResponse(InvocationOnMock i, List<Row> rows) {
    StreamObserver<Response> observer = i.getArgument(1);
    observer.onNext(
        Response.newBuilder().setResultSet(ResultSet.newBuilder().addAllRows(rows)).build());
    observer.onCompleted();
    return null;
  }
}
