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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.grpc.Status;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.PagingPosition;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.RowDecorator;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.TableName;
import io.stargate.grpc.service.GrpcService.EnrichedResponseAndTraceId;
import io.stargate.proto.BridgeQuery.EnrichedResponse;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public abstract class EnrichedQueryHandler extends EnrichedMessageHandler<Query, Prepared> {

  private final String decoratedKeyspace;
  private final SchemaAgreementHelper schemaAgreementHelper;
  private Parameters parameters;

  protected EnrichedQueryHandler(
      Query query,
      Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      ExceptionHandler exceptionHandler) {
    super(query, connection, persistence, exceptionHandler);
    this.schemaAgreementHelper =
        new SchemaAgreementHelper(connection, schemaAgreementRetries, executor);
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
    this.parameters = makeParameters(parameters, connection.clientInfo());
    try {
      return connection.execute(
          bindValues(prepared, message.getValues()), this.parameters, queryStartNanoTime);
    } catch (Exception e) {
      return failedFuture(e, prepared.isIdempotent);
    }
  }

  @Override
  protected CompletionStage<EnrichedResponseAndTraceId> buildResponse(Result result) {
    EnrichedResponse.Builder responseBuilder = makeResponseBuilder(result);
    switch (result.kind) {
      case Void:
        return CompletableFuture.completedFuture(
            EnrichedResponseAndTraceId.from(result, responseBuilder));
      case SchemaChange:
        return schemaAgreementHelper
            .waitForAgreement()
            .thenApply(
                __ -> {
                  SchemaChange schemaChange = buildSchemaChange((Result.SchemaChange) result);
                  responseBuilder.setSchemaChange(schemaChange);
                  return EnrichedResponseAndTraceId.from(result, responseBuilder);
                });
      case Rows:
        try {
          responseBuilder.setResultSet(
              ValuesHelper.processEnrichedResult(
                  (Result.Rows) result,
                  message.getParameters().getSkipMetadata(),
                  this::getComparableBytesFromRow,
                  this::getPagingStateFromRow,
                  this::makeRow));
          return CompletableFuture.completedFuture(
              EnrichedResponseAndTraceId.from(result, responseBuilder));
        } catch (Exception e) {
          return failedFuture(e, false);
        }
      default:
        return failedFuture(
            Status.INTERNAL.withDescription("Unhandled result kind").asException(), false);
    }
  }

  private ByteBuffer getComparableBytesFromRow(List<Column> columns, Row row) {
    RowDecorator rowDecorator = connection.makeRowDecorator(TableName.of(columns));
    return rowDecorator.getComparableBytes(row);
  }

  private ByteBuffer getPagingStateFromRow(Row row) {
    return connection.makePagingState(
        PagingPosition.ofCurrentRow(row).resumeFrom(PagingPosition.ResumeMode.NEXT_ROW).build(),
        parameters);
  }

  private Row makeRow(List<Column> columns, List<ByteBuffer> row) {
    ProtocolVersion driverProtocolVersion = this.parameters.protocolVersion().toDriverVersion();
    return new ArrayListBackedRow(columns, row, driverProtocolVersion);
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
}
