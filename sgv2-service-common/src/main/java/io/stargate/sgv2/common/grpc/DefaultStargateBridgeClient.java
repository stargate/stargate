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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.StargateBridgeGrpc;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Hex;

class DefaultStargateBridgeClient implements StargateBridgeClient {

  private static final int TIMEOUT_SECONDS = 5;
  private static final Metadata.Key<String> HOST_KEY =
      Metadata.Key.of("Host", Metadata.ASCII_STRING_MARSHALLER);

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
  public CqlKeyspaceDescribe getKeyspace(String keyspaceName) {
    // TODO authorize?
    return stripTenantPrefix(schema.getKeyspace(addTenantPrefix(keyspaceName)));
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
      return tenantPrefix;
    }
    return fullKeyspaceName;
  }
}
