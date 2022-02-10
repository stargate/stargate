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
import io.stargate.proto.Schema.AuthorizeSchemaReadsRequest;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.SchemaRead;
import io.stargate.proto.StargateBridgeGrpc;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;

class DefaultStargateBridgeClient implements StargateBridgeClient {

  private static final int TIMEOUT_SECONDS = 5;
  private static final Metadata.Key<String> HOST_KEY =
      Metadata.Key.of("Host", Metadata.ASCII_STRING_MARSHALLER);
  private static final Query SELECT_KEYSPACE_NAMES =
      Query.newBuilder().setCql("SELECT keyspace_name FROM system_schema.keyspaces").build();

  private final Channel channel;
  private final DefaultStargateBridgeSchema schema;
  private final CallOptions callOptions;
  private final String tenantPrefix;

  DefaultStargateBridgeClient(
      Channel channel,
      DefaultStargateBridgeSchema schema,
      String authToken,
      Optional<String> tenantId) {
    this.schema = schema;
    this.channel = tenantId.map(i -> addMetadata(channel, i)).orElse(channel);
    this.callOptions =
        CallOptions.DEFAULT
            .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .withCallCredentials(new StargateBearerToken(authToken));
    this.tenantPrefix = tenantId.map(this::encodeKeyspacePrefix).orElse("");
  }

  @Override
  public Response executeQuery(Query request) {
    Response response =
        ClientCalls.blockingUnaryCall(
            channel, StargateBridgeGrpc.getExecuteQueryMethod(), callOptions, request);
    if (response.hasSchemaChange()) {
      // Ensure we're not holding a stale copy of the impacted keyspace by the time we return
      // control to the caller
      String keyspaceName = addTenantPrefix(response.getSchemaChange().getKeyspace());
      schema.removeKeyspace(keyspaceName);
    }
    return response;
  }

  @Override
  public Response executeBatch(Batch request) {
    return ClientCalls.blockingUnaryCall(
        channel, StargateBridgeGrpc.getExecuteBatchMethod(), callOptions, request);
  }

  @Override
  public CqlKeyspaceDescribe getKeyspace(String keyspaceName) throws UnauthorizedKeyspaceException {
    if (!authorizeSchemaRead(SchemaReads.keyspace(keyspaceName, SchemaRead.SourceApi.REST))) {
      throw new UnauthorizedKeyspaceException(keyspaceName);
    }
    return getAuthorizedKeyspace(keyspaceName);
  }

  private CqlKeyspaceDescribe getAuthorizedKeyspace(String keyspaceName) {
    return stripTenantPrefix(schema.getKeyspace(addTenantPrefix(keyspaceName)));
  }

  @Override
  public List<CqlKeyspaceDescribe> getAllKeyspaces() {
    List<String> names = getKeyspaceNames(new ArrayList<>(), null);
    List<Boolean> authorizations =
        authorizeSchemaReads(
            names.stream()
                .map(n -> SchemaReads.keyspace(n, SchemaRead.SourceApi.REST))
                .collect(Collectors.toList()));
    List<CqlKeyspaceDescribe> keyspaces = new ArrayList<>(names.size());
    int i = 0;
    for (String name : names) {
      CqlKeyspaceDescribe ks;
      // Null check because it could have been deleted since we listed the names
      if (authorizations.get(i) && (ks = getAuthorizedKeyspace(name)) != null) {
        keyspaces.add(ks);
      }
    }
    return keyspaces;
  }

  @Override
  public List<Boolean> authorizeSchemaReads(List<SchemaRead> schemaReads) {
    return ClientCalls.blockingUnaryCall(
            channel,
            StargateBridgeGrpc.getAuthorizeSchemaReadsMethod(),
            callOptions,
            AuthorizeSchemaReadsRequest.newBuilder().addAllSchemaReads(schemaReads).build())
        .getAuthorizedList();
  }

  private List<String> getKeyspaceNames(List<String> accumulator, BytesValue pagingState) {
    Query query =
        (pagingState == null)
            ? SELECT_KEYSPACE_NAMES
            : Query.newBuilder(SELECT_KEYSPACE_NAMES)
                .setParameters(QueryParameters.newBuilder().setPagingState(pagingState).build())
                .build();
    ResultSet resultSet = executeQuery(query).getResultSet();
    for (Row row : resultSet.getRowsList()) {
      accumulator.add(row.getValues(0).getString());
    }
    return (resultSet.hasPagingState())
        ? getKeyspaceNames(accumulator, resultSet.getPagingState())
        : accumulator;
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
