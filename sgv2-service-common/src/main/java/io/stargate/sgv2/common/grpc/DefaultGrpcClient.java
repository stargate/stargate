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
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.StargateBridgeGrpc;
import java.util.concurrent.TimeUnit;

class DefaultGrpcClient implements GrpcClient {

  private static final int TIMEOUT_SECONDS = 5;

  private final Channel channel;
  private final DefaultGrpcSchema schema;
  private final CallOptions callOptions;

  DefaultGrpcClient(
      Channel channel, DefaultGrpcSchema schema, String authToken, Metadata metadata) {
    this.schema = schema;
    this.channel =
        metadata == null
            ? channel
            : ClientInterceptors.intercept(
                channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
    this.callOptions =
        CallOptions.DEFAULT
            .withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .withCallCredentials(new StargateBearerToken(authToken));
  }

  @Override
  public Response executeQuery(Query request) {
    Response response =
        ClientCalls.blockingUnaryCall(
            channel, StargateBridgeGrpc.getExecuteQueryMethod(), callOptions, request);
    if (response.hasSchemaChange()) {
      // Ensure the impacted keyspace is fresh before we return control to the caller
      String keyspaceName = decorateKeyspaceName(response.getSchemaChange().getKeyspace());
      schema.forceKeyspaceRefresh(keyspaceName);
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
    return schema.getKeyspace(decorateKeyspaceName(keyspaceName));
  }

  private String decorateKeyspaceName(String keyspaceName) {
    // TODO implement (extract tenant name from metadata)
    return keyspaceName;
  }
}
