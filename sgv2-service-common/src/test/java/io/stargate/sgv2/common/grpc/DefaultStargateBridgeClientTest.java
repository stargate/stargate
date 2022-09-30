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
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Int32Value;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.Schema.AuthorizeSchemaReadsRequest;
import io.stargate.bridge.proto.Schema.AuthorizeSchemaReadsResponse;
import io.stargate.bridge.proto.Schema.CqlKeyspace;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.bridge.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.bridge.proto.Schema.SchemaRead;
import io.stargate.bridge.proto.Schema.SchemaRead.SourceApi;
import io.stargate.bridge.proto.Schema.SupportedFeaturesResponse;
import io.stargate.bridge.proto.StargateBridgeGrpc.StargateBridgeImplBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
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
  private Cache<String, CqlKeyspaceDescribe> keyspaceCache;

  @BeforeEach
  public void setup() throws IOException {
    server =
        InProcessServerBuilder.forName(SERVER_NAME).directExecutor().addService(service).build();
    server.start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    keyspaceCache = Caffeine.newBuilder().build();
  }

  @AfterEach
  public void teardown() {
    server.shutdownNow();
    channel.shutdownNow();
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
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName);
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);

    // When
    Optional<CqlKeyspaceDescribe> keyspace = newClient().getKeyspace(keyspaceName, true);

    // Then
    assertThat(keyspace).hasValue(bridgeKeyspace);
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace);
  }

  @Test
  public void getKeyspaceNoAuthCheck() {
    // Given
    String keyspaceName = "ks";
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName);
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);

    // When
    Optional<CqlKeyspaceDescribe> keyspace = newClient().getKeyspace(keyspaceName, false);

    // Then
    assertThat(keyspace).hasValue(bridgeKeyspace);
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace);
    verify(service, never()).authorizeSchemaReads(any(), any());
  }

  @Test
  public void getKeyspaceWhenNonExistent() {
    // Given
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    mockDescribeNotFound(keyspaceName, Optional.empty());

    // When
    Optional<CqlKeyspaceDescribe> keyspace = newClient().getKeyspace(keyspaceName, true);

    // Then
    assertThat(keyspace).isEmpty();
  }

  @Test
  public void getKeyspaceWhenAlreadyCached() {
    // Given
    DefaultStargateBridgeClient client = newClient();
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName);
    // first bridge call fetches and populates the cache
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);
    client.getKeyspace(keyspaceName, true);
    verify(service).describeKeyspace(eq(describeQuery(keyspaceName)), any());
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace);
    // second bridge call will only check the hash
    mockDescribeUnchanged(keyspaceName, bridgeKeyspace.getHash().getValue());

    // When
    Optional<CqlKeyspaceDescribe> keyspace = client.getKeyspace(keyspaceName, true);

    // Then
    verify(service)
        .describeKeyspace(
            eq(describeQuery(keyspaceName, bridgeKeyspace.getHash().getValue())), any());
    assertThat(keyspace).hasValue(bridgeKeyspace);
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace);
  }

  @Test
  public void getKeyspaceWhenCachedButHasChanged() {
    // Given
    DefaultStargateBridgeClient client = newClient();
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    CqlKeyspaceDescribe bridgeKeyspace1 = buildKeyspace(keyspaceName);
    // first bridge call fetches and populates the cache
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace1);
    client.getKeyspace(keyspaceName, true);
    verify(service).describeKeyspace(eq(describeQuery(keyspaceName)), any());
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace1);
    // second bridge call will check the hash and find out a new version exists
    CqlKeyspaceDescribe bridgeKeyspace2 = buildKeyspace(keyspaceName, "tbl1");
    mockDescribeResponse(
        keyspaceName, Optional.of(bridgeKeyspace1.getHash().getValue()), bridgeKeyspace2);

    // When
    Optional<CqlKeyspaceDescribe> keyspace = client.getKeyspace(keyspaceName, true);

    // Then
    verify(service)
        .describeKeyspace(
            eq(describeQuery(keyspaceName, bridgeKeyspace1.getHash().getValue())), any());
    assertThat(keyspace).hasValue(bridgeKeyspace2);
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace2);
  }

  @Test
  public void getKeyspaceWhenCachedButWasDeleted() {
    // Given
    DefaultStargateBridgeClient client = newClient();
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), true);
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName);
    // first bridge call fetches and populates the cache
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);
    client.getKeyspace(keyspaceName, true);
    verify(service).describeKeyspace(eq(describeQuery(keyspaceName)), any());
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isEqualTo(bridgeKeyspace);
    // second bridge call will check the hash and find out the keyspace is gone
    mockDescribeNotFound(keyspaceName, Optional.of(bridgeKeyspace.getHash().getValue()));

    // When
    Optional<CqlKeyspaceDescribe> keyspace = client.getKeyspace(keyspaceName, true);

    // Then
    verify(service)
        .describeKeyspace(
            eq(describeQuery(keyspaceName, bridgeKeyspace.getHash().getValue())), any());
    assertThat(keyspace).isEmpty();
    assertThat(keyspaceCache.getIfPresent(keyspaceName)).isNull();
  }

  @Test
  public void getKeyspaceUnauthorized() {
    // Given
    String keyspaceName = "ks";
    mockAuthorization(SchemaReads.keyspace(keyspaceName, SOURCE_API), false);

    // Then
    assertThatThrownBy(() -> newClient().getKeyspace(keyspaceName, true))
        .isInstanceOf(UnauthorizedKeyspaceException.class);
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
    CqlKeyspaceDescribe bridgeKeyspace1 = buildKeyspace("ks1");
    mockDescribeResponse("ks1", Optional.empty(), bridgeKeyspace1);
    CqlKeyspaceDescribe bridgeKeyspace3 = buildKeyspace("ks3");
    mockDescribeResponse("ks3", Optional.empty(), bridgeKeyspace3);

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
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName, tableName);
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);

    // When
    Optional<CqlTable> table = newClient().getTable(keyspaceName, tableName, true);

    // Then
    assertThat(table).hasValueSatisfying(t -> assertThat(t.getName()).isEqualTo(tableName));
  }

  @Test
  public void getTableNoAuthCheck() {
    // Given
    String keyspaceName = "ks";
    String tableName = "tbl";
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName, tableName);
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);

    // When
    Optional<CqlTable> table = newClient().getTable(keyspaceName, tableName, false);

    // Then
    assertThat(table).hasValueSatisfying(t -> assertThat(t.getName()).isEqualTo(tableName));
    verify(service, never()).authorizeSchemaReads(any(), any());
  }

  @Test
  public void getTableNonExistent() {
    // Given
    String keyspaceName = "ks";
    String tableName = "tbl";
    mockAuthorization(SchemaReads.table(keyspaceName, tableName, SOURCE_API), true);
    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName);
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);

    // When
    Optional<CqlTable> table = newClient().getTable(keyspaceName, tableName, true);

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
    assertThatThrownBy(() -> newClient().getTable(keyspaceName, tableName, true))
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

    CqlKeyspaceDescribe bridgeKeyspace = buildKeyspace(keyspaceName, "tbl1", "tbl2", "tbl3");
    mockDescribeResponse(keyspaceName, Optional.empty(), bridgeKeyspace);

    // When
    List<CqlTable> tables = newClient().getTables(keyspaceName);

    // Then
    assertThat(tables).extracting(CqlTable::getName).contains("tbl1", "tbl3");
  }

  @ParameterizedTest
  @MethodSource("getSupportedFeatures")
  public void shouldGetSupportedFeatures(
      boolean secondaryIndexes, boolean sai, boolean loggedBatches) {
    // Given
    mockSupportedFeatures(secondaryIndexes, sai, loggedBatches);

    // When
    SupportedFeaturesResponse response = newClient().getSupportedFeatures();

    // Then
    assertThat(response.getSecondaryIndexes()).isEqualTo(secondaryIndexes);
    assertThat(response.getSai()).isEqualTo(sai);
    assertThat(response.getLoggedBatches()).isEqualTo(loggedBatches);
  }

  public static Arguments[] getSupportedFeatures() {
    return new Arguments[] {
      arguments(false, false, false),
      arguments(false, false, true),
      arguments(false, true, false),
      arguments(false, true, true),
      arguments(true, false, false),
      arguments(true, false, true),
      arguments(true, true, false),
      arguments(true, true, true),
    };
  }

  private DefaultStargateBridgeClient newClient() {
    return new DefaultStargateBridgeClient(
        channel, AUTH_TOKEN, Optional.empty(), 5, keyspaceCache, new LazyReference<>(), SOURCE_API);
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

  private CqlKeyspaceDescribe buildKeyspace(String keyspaceName, String... tableNames) {
    // Simulate a distinct hash (would normally be generated on the bridge side)
    int hash = Objects.hash(keyspaceName, Arrays.hashCode(tableNames));

    return CqlKeyspaceDescribe.newBuilder()
        .setCqlKeyspace(CqlKeyspace.newBuilder().setName(keyspaceName))
        .addAllTables(
            Arrays.stream(tableNames)
                .map(n -> CqlTable.newBuilder().setName(n).build())
                .collect(Collectors.toList()))
        .setHash(Int32Value.of(hash))
        .build();
  }

  private void mockDescribeResponse(
      String keyspaceName, Optional<Integer> hash, CqlKeyspaceDescribe response) {
    DescribeKeyspaceQuery request =
        hash.map(h -> describeQuery(keyspaceName, h)).orElse(describeQuery(keyspaceName));
    doAnswer(i -> mockResponse(i, response)).when(service).describeKeyspace(eq(request), any());
  }

  private void mockDescribeNotFound(String keyspaceName, Optional<Integer> hash) {
    DescribeKeyspaceQuery request =
        hash.map(h -> describeQuery(keyspaceName, h)).orElse(describeQuery(keyspaceName));
    doAnswer(i -> mockError(i, Status.NOT_FOUND.withDescription("Keyspace not found")))
        .when(service)
        .describeKeyspace(eq(request), any());
  }

  private void mockDescribeUnchanged(String keyspaceName, int hash) {
    DescribeKeyspaceQuery request = describeQuery(keyspaceName, hash);
    CqlKeyspaceDescribe emptyResponse = CqlKeyspaceDescribe.newBuilder().build();
    doAnswer(i -> mockResponse(i, emptyResponse))
        .when(service)
        .describeKeyspace(eq(request), any());
  }

  private DescribeKeyspaceQuery describeQuery(String keyspaceName) {
    return DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();
  }

  private DescribeKeyspaceQuery describeQuery(String keyspaceName, int hash) {
    return DescribeKeyspaceQuery.newBuilder()
        .setKeyspaceName(keyspaceName)
        .setHash(Int32Value.of(hash))
        .build();
  }

  private void mockQuery(Query query, List<Row> rows) {
    doAnswer(i -> mockRows(i, rows)).when(service).executeQuery(eq(query), any());
  }

  private void mockBatch(Batch batch, List<Row> rows) {
    doAnswer(i -> mockRows(i, rows)).when(service).executeBatch(eq(batch), any());
  }

  private void mockSupportedFeatures(boolean secondaryIndexes, boolean sai, boolean loggedBatches) {
    doAnswer(
            i ->
                mockResponse(
                    i,
                    SupportedFeaturesResponse.newBuilder()
                        .setSecondaryIndexes(secondaryIndexes)
                        .setSai(sai)
                        .setLoggedBatches(loggedBatches)
                        .build()))
        .when(service)
        .getSupportedFeatures(any(), any());
  }

  private Void mockRows(InvocationOnMock i, List<Row> rows) {
    return mockResponse(
        i, Response.newBuilder().setResultSet(ResultSet.newBuilder().addAllRows(rows)).build());
  }

  private <T> Void mockResponse(InvocationOnMock i, T response) {
    StreamObserver<T> observer = i.getArgument(1);
    observer.onNext(response);
    observer.onCompleted();
    return null;
  }

  private <T> Void mockError(InvocationOnMock i, Status error) {
    StreamObserver<T> observer = i.getArgument(1);
    observer.onError(error.asException());
    return null;
  }
}
