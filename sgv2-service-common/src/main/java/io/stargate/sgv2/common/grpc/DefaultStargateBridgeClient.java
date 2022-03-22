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

import com.google.protobuf.BytesValue;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.AuthorizeSchemaReadsRequest;
import io.stargate.proto.Schema.AuthorizeSchemaReadsResponse;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.SchemaRead;
import io.stargate.proto.Schema.SchemaRead.SourceApi;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.sgv2.common.futures.Futures;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;

class DefaultStargateBridgeClient implements StargateBridgeClient {

  private static final int TIMEOUT_SECONDS = 5;
  private static final Metadata.Key<String> HOST_KEY =
      Metadata.Key.of("Host", Metadata.ASCII_STRING_MARSHALLER);
  static final Query SELECT_KEYSPACE_NAMES =
      Query.newBuilder().setCql("SELECT keyspace_name FROM system_schema.keyspaces").build();

  private final Channel channel;
  private final DefaultStargateBridgeSchema schema;
  private final CallOptions callOptions;
  private final String tenantPrefix;
  private final SourceApi sourceApi;

  DefaultStargateBridgeClient(
      Channel channel,
      DefaultStargateBridgeSchema schema,
      String authToken,
      Optional<String> tenantId,
      SourceApi sourceApi) {
    this.schema = schema;
    this.channel = tenantId.map(i -> addMetadata(channel, i)).orElse(channel);
    this.callOptions =
        CallOptions.DEFAULT
            .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .withCallCredentials(new StargateBearerToken(authToken));
    this.tenantPrefix = tenantId.map(this::encodeKeyspacePrefix).orElse("");
    this.sourceApi = sourceApi;
  }

  @Override
  public CompletionStage<Response> executeQueryAsync(Query query) {
    ClientCall<Query, Response> call =
        channel.newCall(StargateBridgeGrpc.getExecuteQueryMethod(), callOptions);
    UnaryStreamObserver<Response> observer = new UnaryStreamObserver<>();
    ClientCalls.asyncUnaryCall(call, query, observer);
    return observer
        .asFuture()
        .thenApply(
            response -> {
              if (response.hasSchemaChange()) {
                // Ensure we're not holding a stale copy of the impacted keyspace by the time we
                // return control to the caller
                String keyspaceName = addTenantPrefix(response.getSchemaChange().getKeyspace());
                schema.removeKeyspace(keyspaceName);
              }
              return response;
            });
  }

  @Override
  public CompletionStage<Response> executeBatchAsync(Batch batch) {
    ClientCall<Batch, Response> call =
        channel.newCall(StargateBridgeGrpc.getExecuteBatchMethod(), callOptions);
    UnaryStreamObserver<Response> observer = new UnaryStreamObserver<>();
    ClientCalls.asyncUnaryCall(call, batch, observer);
    return observer.asFuture();
  }

  @Override
  public CompletionStage<Optional<CqlKeyspaceDescribe>> getKeyspaceAsync(String keyspaceName) {
    return authorizeSchemaReadAsync(SchemaReads.keyspace(keyspaceName, sourceApi))
        .thenCompose(
            authorized -> {
              if (!authorized) {
                throw new UnauthorizedKeyspaceException(keyspaceName);
              }
              return getAuthorizedKeyspace(keyspaceName);
            });
  }

  private CompletionStage<Optional<CqlKeyspaceDescribe>> getAuthorizedKeyspace(
      String keyspaceName) {
    return schema
        .getKeyspaceAsync(addTenantPrefix(keyspaceName))
        .thenApply(ks -> Optional.ofNullable(stripTenantPrefix(ks)));
  }

  @Override
  public CompletionStage<List<CqlKeyspaceDescribe>> getAllKeyspacesAsync() {
    CompletionStage<List<String>> allNamesFuture = getKeyspaceNames(new ArrayList<>(), null);
    CompletionStage<List<String>> authorizedNamesFuture =
        allNamesFuture.thenCompose(
            names -> {
              List<SchemaRead> reads =
                  names.stream()
                      .map(n -> SchemaReads.keyspace(n, sourceApi))
                      .collect(Collectors.toList());
              return authorizeSchemaReadsAsync(reads)
                  .thenApply(authorizations -> removeUnauthorized(names, authorizations));
            });
    return authorizedNamesFuture.thenCompose(this::getAuthorizedkeyspaces);
  }

  private <T> List<T> removeUnauthorized(List<T> elements, List<Boolean> authorizations) {
    assert elements.size() == authorizations.size();
    List<T> filtered = new ArrayList<>(elements.size());
    for (int i = 0; i < elements.size(); i++) {
      if (authorizations.get(i)) {
        filtered.add(elements.get(i));
      }
    }
    return filtered;
  }

  private CompletionStage<List<CqlKeyspaceDescribe>> getAuthorizedkeyspaces(
      List<String> keyspaceNames) {
    List<CompletionStage<CqlKeyspaceDescribe>> keyspaceFutures =
        keyspaceNames.stream()
            .map(keyspaceName -> schema.getKeyspaceAsync(addTenantPrefix(keyspaceName)))
            .collect(Collectors.toList());
    // Filter out nulls in case any keyspace was removed since we fetched the names
    return Futures.sequence(keyspaceFutures)
        .thenApply(
            keyspaces -> keyspaces.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  @Override
  public String decorateKeyspaceName(String keyspaceName) {
    return addTenantPrefix(keyspaceName);
  }

  @Override
  public CompletionStage<Optional<Schema.CqlTable>> getTableAsync(
      String keyspaceName, String tableName) {
    return authorizeSchemaReadAsync(SchemaReads.table(keyspaceName, tableName, sourceApi))
        .thenCompose(
            authorized -> {
              if (!authorized) {
                throw new UnauthorizedTableException(keyspaceName, tableName);
              }
              return getAuthorizedKeyspace(keyspaceName)
                  .thenApply(
                      maybeKs ->
                          maybeKs.flatMap(
                              ks ->
                                  ks.getTablesList().stream()
                                      .filter(t -> t.getName().equals(tableName))
                                      .findFirst()));
            });
  }

  @Override
  public CompletionStage<List<Schema.CqlTable>> getTablesAsync(String keyspaceName) {
    return getAuthorizedKeyspace(keyspaceName)
        .thenCompose(
            maybeKeyspace ->
                maybeKeyspace
                    .map(
                        keyspace -> {
                          List<Schema.CqlTable> tables = keyspace.getTablesList();
                          List<SchemaRead> reads =
                              tables.stream()
                                  .map(t -> SchemaReads.table(keyspaceName, t.getName(), sourceApi))
                                  .collect(Collectors.toList());
                          return authorizeSchemaReadsAsync(reads)
                              .thenApply(
                                  authorizations -> removeUnauthorized(tables, authorizations));
                        })
                    .orElse(CompletableFuture.completedFuture(Collections.emptyList())));
  }

  @Override
  public CompletionStage<List<Boolean>> authorizeSchemaReadsAsync(List<SchemaRead> schemaReads) {
    ClientCall<AuthorizeSchemaReadsRequest, AuthorizeSchemaReadsResponse> call =
        channel.newCall(StargateBridgeGrpc.getAuthorizeSchemaReadsMethod(), callOptions);
    UnaryStreamObserver<AuthorizeSchemaReadsResponse> observer = new UnaryStreamObserver<>();
    ClientCalls.asyncUnaryCall(
        call,
        AuthorizeSchemaReadsRequest.newBuilder().addAllSchemaReads(schemaReads).build(),
        observer);
    return observer.asFuture().thenApply(AuthorizeSchemaReadsResponse::getAuthorizedList);
  }

  private CompletionStage<List<String>> getKeyspaceNames(
      List<String> accumulator, BytesValue pagingState) {
    Query query =
        (pagingState == null)
            ? SELECT_KEYSPACE_NAMES
            : Query.newBuilder(SELECT_KEYSPACE_NAMES)
                .setParameters(QueryParameters.newBuilder().setPagingState(pagingState).build())
                .build();
    return executeQueryAsync(query)
        .thenCompose(
            response -> {
              ResultSet resultSet = response.getResultSet();
              for (Row row : resultSet.getRowsList()) {
                accumulator.add(row.getValues(0).getString());
              }
              return (resultSet.hasPagingState())
                  ? getKeyspaceNames(accumulator, resultSet.getPagingState())
                  : CompletableFuture.completedFuture(accumulator);
            });
  }

  private Channel addMetadata(Channel channel, String tenantId) {
    Metadata metadata = new Metadata();
    metadata.put(HOST_KEY, tenantId);
    return ClientInterceptors.intercept(
        channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private String encodeKeyspacePrefix(String tenantId) {
    return Hex.encodeHexString(tenantId.getBytes(StandardCharsets.UTF_8)) + "_";
  }

  private String addTenantPrefix(String keyspaceName) {
    return tenantPrefix + keyspaceName;
  }

  private CqlKeyspaceDescribe stripTenantPrefix(CqlKeyspaceDescribe describe) {
    if (describe == null || tenantPrefix.isEmpty()) {
      return describe;
    }
    CqlKeyspace keyspace = describe.getCqlKeyspace();
    return CqlKeyspaceDescribe.newBuilder(describe)
        .setCqlKeyspace(
            CqlKeyspace.newBuilder(keyspace).setName(stripTenantPrefix(keyspace.getName())).build())
        .build();
  }

  private String stripTenantPrefix(String fullKeyspaceName) {
    if (fullKeyspaceName.startsWith(tenantPrefix)) {
      return fullKeyspaceName.substring(tenantPrefix.length());
    }
    return fullKeyspaceName;
  }
}
