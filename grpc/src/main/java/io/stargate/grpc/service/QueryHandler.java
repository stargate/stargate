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
package io.stargate.grpc.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.protobuf.StringValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.grpc.service.Service.PrepareInfo;
import io.stargate.grpc.service.Service.ResponseAndTraceId;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

class QueryHandler extends MessageHandler<Query, Prepared> {

  private final PrepareInfo prepareInfo;

  QueryHandler(
      Query query,
      Connection connection,
      Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    super(query, connection, preparedCache, persistence, responseObserver);
    QueryParameters queryParameters = query.getParameters();
    this.prepareInfo =
        ImmutablePrepareInfo.builder()
            .keyspace(
                queryParameters.hasKeyspace() ? queryParameters.getKeyspace().getValue() : null)
            .user(connection.loggedUser().map(AuthenticatedUser::name).orElse(null))
            .cql(query.getCql())
            .build();
  }

  @Override
  protected void validate() {
    // nothing to do
  }

  @Override
  protected CompletionStage<Prepared> prepare(boolean shouldInvalidate) {
    return prepare(prepareInfo, shouldInvalidate);
  }

  @Override
  protected CompletionStage<ResultAndIdempotencyInfo> executePrepared(Prepared prepared) {
    long queryStartNanoTime = System.nanoTime();

    Payload values = message.getValues();
    PayloadHandler handler = PayloadHandlers.get(values.getType());

    QueryParameters parameters = message.getParameters();

    try {
      return connection
          .execute(
              bindValues(handler, prepared, values),
              makeParameters(parameters, connection.clientInfo()),
              queryStartNanoTime)
          .thenApply(r -> new ResultAndIdempotencyInfo(r, prepared.isIdempotent));
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  @Override
  protected ResponseAndTraceId buildResponse(ResultAndIdempotencyInfo resultAndIdempotencyInfo) {
    Result result = resultAndIdempotencyInfo.result;
    ResponseAndTraceId responseAndTraceId = new ResponseAndTraceId();
    responseAndTraceId.setTracingId(result.getTracingId());
    Response.Builder responseBuilder = makeResponseBuilder(result);
    responseBuilder.setIsIdempotent(resultAndIdempotencyInfo.isIdempotent);
    switch (result.kind) {
      case Void:
        break;
      case SchemaChange:
        // TODO make this non-blocking (see #1145)
        persistence.waitForSchemaAgreement();
        Result.SchemaChangeMetadata metadata = ((Result.SchemaChange) result).metadata;
        SchemaChange.Builder schemaChangeBuilder =
            SchemaChange.newBuilder()
                .setChangeType(SchemaChange.Type.valueOf(metadata.change))
                .setTarget(SchemaChange.Target.valueOf(metadata.target))
                .setKeyspace(metadata.keyspace);
        if (metadata.name != null) {
          schemaChangeBuilder.setName(StringValue.of(metadata.name));
        }
        if (metadata.argTypes != null) {
          schemaChangeBuilder.addAllArgumentTypes(metadata.argTypes);
        }
        responseBuilder.setSchemaChange(schemaChangeBuilder.build());
        break;
      case Rows:
        Payload values = message.getValues();
        PayloadHandler handler = PayloadHandlers.get(values.getType());
        try {
          responseBuilder.setResultSet(
              Payload.newBuilder()
                  .setType(message.getValues().getType())
                  .setData(handler.processResult((Result.Rows) result, message.getParameters())));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
        break;
      case SetKeyspace:
        throw new CompletionException(
            Status.INVALID_ARGUMENT.withDescription("USE <keyspace> not supported").asException());
      default:
        throw new CompletionException(
            Status.INTERNAL.withDescription("Unhandled result kind").asException());
    }
    responseAndTraceId.setResponseBuilder(responseBuilder);
    return responseAndTraceId;
  }

  @Override
  protected ConsistencyLevel getTracingConsistency() {
    QueryParameters parameters = message.getParameters();
    return parameters.hasTracingConsistency()
        ? ConsistencyLevel.fromCode(parameters.getTracingConsistency().getValue().getNumber())
        : MessageHandler.DEFAULT_TRACING_CONSISTENCY;
  }

  private Parameters makeParameters(QueryParameters parameters, Optional<ClientInfo> clientInfo) {
    ImmutableParameters.Builder builder = ImmutableParameters.builder();

    builder.consistencyLevel(
        parameters.hasConsistency()
            ? ConsistencyLevel.fromCode(parameters.getConsistency().getValue().getNumber())
            : Service.DEFAULT_CONSISTENCY);

    if (parameters.hasKeyspace()) {
      builder.defaultKeyspace(parameters.getKeyspace().getValue());
    }

    builder.pageSize(
        parameters.hasPageSize() ? parameters.getPageSize().getValue() : Service.DEFAULT_PAGE_SIZE);

    if (parameters.hasPagingState()) {
      builder.pagingState(ByteBuffer.wrap(parameters.getPagingState().getValue().toByteArray()));
    }

    builder.serialConsistencyLevel(
        parameters.hasSerialConsistency()
            ? ConsistencyLevel.fromCode(parameters.getSerialConsistency().getValue().getNumber())
            : Service.DEFAULT_SERIAL_CONSISTENCY);

    if (parameters.hasTimestamp()) {
      builder.defaultTimestamp(parameters.getTimestamp().getValue());
    }

    if (parameters.hasNowInSeconds()) {
      builder.nowInSeconds(parameters.getNowInSeconds().getValue());
    }

    clientInfo.ifPresent(
        c -> {
          Map<String, ByteBuffer> customPayload = new HashMap<>();
          c.storeAuthenticationData(customPayload);
          builder.customPayload(customPayload);
        });

    return builder.tracingRequested(parameters.getTracing()).build();
  }
}
