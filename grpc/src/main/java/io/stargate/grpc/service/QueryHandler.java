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

import com.google.protobuf.StringValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.grpc.service.GrpcService.ResponseAndTraceId;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

class QueryHandler extends MessageHandler<Query, Prepared> {

  private final String decoratedKeyspace;
  private final ScheduledExecutorService executor;
  private final int schemaAgreementRetries;

  QueryHandler(
      Query query,
      Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<Response> responseObserver,
      ExceptionHandler exceptionHandler) {
    super(query, connection, persistence, responseObserver, exceptionHandler);
    this.executor = executor;
    this.schemaAgreementRetries = schemaAgreementRetries;
    QueryParameters queryParameters = query.getParameters();
    this.decoratedKeyspace =
        queryParameters.hasKeyspace()
            ? persistence.decorateKeyspaceName(
                queryParameters.getKeyspace().getValue(), GrpcService.HEADERS_KEY.get())
            : null;
  }

  @Override
  protected void validate() {
    // nothing to do
  }

  @Override
  protected CompletionStage<Prepared> prepare() {
    return prepare(message.getCql(), decoratedKeyspace);
  }

  @Override
  protected CompletionStage<Result> executePrepared(Prepared prepared) {
    long queryStartNanoTime = System.nanoTime();

    QueryParameters parameters = message.getParameters();
    try {
      return connection.execute(
          bindValues(prepared, message.getValues()),
          makeParameters(parameters, connection.clientInfo()),
          queryStartNanoTime);
    } catch (Exception e) {
      return failedFuture(e, prepared.isIdempotent);
    }
  }

  @Override
  protected CompletionStage<ResponseAndTraceId> buildResponse(Result result) {
    Response.Builder responseBuilder = makeResponseBuilder(result);
    switch (result.kind) {
      case Void:
        return CompletableFuture.completedFuture(ResponseAndTraceId.from(result, responseBuilder));
      case SchemaChange:
        return waitForSchemaAgreement()
            .thenApply(
                __ -> {
                  SchemaChange schemaChange = buildSchemaChange((Result.SchemaChange) result);
                  responseBuilder.setSchemaChange(schemaChange);
                  return ResponseAndTraceId.from(result, responseBuilder);
                });
      case Rows:
        try {
          responseBuilder.setResultSet(
              ValuesHelper.processResult((Result.Rows) result, message.getParameters()));
          return CompletableFuture.completedFuture(
              ResponseAndTraceId.from(result, responseBuilder));
        } catch (Exception e) {
          return failedFuture(e, false);
        }
      default:
        return failedFuture(
            Status.INTERNAL.withDescription("Unhandled result kind").asException(), false);
    }
  }

  private SchemaChange buildSchemaChange(Result.SchemaChange result) {
    Result.SchemaChangeMetadata metadata = result.metadata;
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
    return schemaChangeBuilder.build();
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
            : GrpcService.DEFAULT_CONSISTENCY);

    if (decoratedKeyspace != null) {
      builder.defaultKeyspace(decoratedKeyspace);
    }

    builder.pageSize(
        parameters.hasPageSize()
            ? parameters.getPageSize().getValue()
            : GrpcService.DEFAULT_PAGE_SIZE);

    if (parameters.hasPagingState()) {
      builder.pagingState(ByteBuffer.wrap(parameters.getPagingState().getValue().toByteArray()));
    }

    builder.serialConsistencyLevel(
        parameters.hasSerialConsistency()
            ? ConsistencyLevel.fromCode(parameters.getSerialConsistency().getValue().getNumber())
            : GrpcService.DEFAULT_SERIAL_CONSISTENCY);

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

  private CompletionStage<Void> waitForSchemaAgreement() {
    CompletableFuture<Void> agreementFuture = new CompletableFuture<>();
    waitForSchemaAgreement(schemaAgreementRetries, agreementFuture);
    return agreementFuture;
  }

  private void waitForSchemaAgreement(
      int remainingAttempts, CompletableFuture<Void> agreementFuture) {
    if (connection.isInSchemaAgreement()) {
      agreementFuture.complete(null);
      return;
    }
    if (remainingAttempts <= 1) {
      agreementFuture.completeExceptionally(
          Status.DEADLINE_EXCEEDED
              .withDescription(
                  "Failed to reach schema agreement after "
                      + (200 * schemaAgreementRetries)
                      + " milliseconds.")
              .asException());
      return;
    }
    executor.schedule(
        () -> waitForSchemaAgreement(remainingAttempts - 1, agreementFuture),
        200,
        TimeUnit.MILLISECONDS);
  }
}
