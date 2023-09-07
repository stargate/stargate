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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.auth.SourceAPI;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.QueryOuterClass.SchemaChange;
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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class QueryHandler extends MessageHandler<Query, Prepared> {

  private final String decoratedKeyspace;
  private final SchemaAgreementHelper schemaAgreementHelper;
  private final boolean enrichResponse;
  private final SourceAPI sourceAPI;
  private volatile Parameters parameters;
  public static final ByteBuffer EXHAUSTED_PAGE_STATE = ByteBuffer.allocate(0);

  QueryHandler(
      Query query,
      Connection connection,
      Persistence persistence,
      SourceAPI sourceAPI,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<Response> responseObserver) {
    super(query, connection, persistence, responseObserver);
    this.schemaAgreementHelper =
        new SchemaAgreementHelper(connection, schemaAgreementRetries, executor);
    QueryParameters queryParameters = query.getParameters();
    Map<String, String> headers = BridgeService.HEADERS_KEY.get();
    this.decoratedKeyspace =
        queryParameters.hasKeyspace()
            ? persistence.decorateKeyspaceName(queryParameters.getKeyspace().getValue(), headers)
            : null;
    this.enrichResponse = query.hasParameters() && query.getParameters().getEnriched();
    this.sourceAPI = sourceAPI;
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
      this.parameters = makeParameters(parameters, connection.clientInfo());
      return connection.execute(
          bindValues(prepared, message.getValues()), this.parameters, queryStartNanoTime);
    } catch (Exception e) {
      return failedFuture(e, prepared.isIdempotent);
    }
  }

  @Override
  protected CompletionStage<BridgeService.ResponseAndTraceId> buildResponse(Result result) {
    Response.Builder responseBuilder = makeResponseBuilder(result);
    switch (result.kind) {
      case Void:
        return CompletableFuture.completedFuture(
            BridgeService.ResponseAndTraceId.from(result, responseBuilder));
      case SchemaChange:
        return schemaAgreementHelper
            .waitForAgreement()
            .thenApply(
                __ -> {
                  SchemaChange schemaChange = buildSchemaChange((Result.SchemaChange) result);
                  responseBuilder.setSchemaChange(schemaChange);
                  return BridgeService.ResponseAndTraceId.from(result, responseBuilder);
                });
      case Rows:
        try {
          Result.Rows rows = (Result.Rows) result;

          if (enrichResponse) {
            // TODO raw use of column. Is this correct?
            RowDecorator rowDecorator =
                connection.makeRowDecorator(TableName.of(rows.resultMetadata.columns));
            responseBuilder.setResultSet(
                ValuesHelper.processResult(
                    rows,
                    message.getParameters(),
                    this::getComparableBytesFromRow,
                    this::getPagingStateFromRow,
                    this::makeRow,
                    rowDecorator));
          } else {
            responseBuilder.setResultSet(ValuesHelper.processResult(rows, message.getParameters()));
          }
          return CompletableFuture.completedFuture(
              BridgeService.ResponseAndTraceId.from(result, responseBuilder));
        } catch (Exception e) {
          return failedFuture(e, false);
        }
      default:
        return failedFuture(
            Status.INTERNAL.withDescription("Unhandled result kind").asException(), false);
    }
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private ByteBuffer getComparableBytesFromRow(
      List<Column> columns, Row row, RowDecorator rowDecorator) {
    return rowDecorator.getComparableBytes(row);
  }

  private ByteBuffer getPagingStateFromRow(
      ByteBuffer resultSetPagingState,
      Row row,
      QueryOuterClass.ResumeMode resumeMode,
      boolean lastInPage) {

    // if we don't have resume mode, return null
    if (resumeMode == null || resumeMode == QueryOuterClass.ResumeMode.UNRECOGNIZED) {
      return null;
    }

    // otherwise, if last and no paging state exhausted
    if (lastInPage && resultSetPagingState == null) {
      return EXHAUSTED_PAGE_STATE;
    }

    // otherwise resolve to internal and make
    PagingPosition.ResumeMode internalResumeMode = PagingPosition.ResumeMode.NEXT_ROW;
    if (resumeMode == QueryOuterClass.ResumeMode.NEXT_PARTITION) {
      internalResumeMode = PagingPosition.ResumeMode.NEXT_PARTITION;
    }

    return connection.makePagingState(
        PagingPosition.ofCurrentRow(row).resumeFrom(internalResumeMode).build(), this.parameters);
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
            : BridgeService.DEFAULT_CONSISTENCY);

    if (decoratedKeyspace != null) {
      builder.defaultKeyspace(decoratedKeyspace);
    }

    builder.pageSize(
        parameters.hasPageSize()
            ? parameters.getPageSize().getValue()
            : BridgeService.DEFAULT_PAGE_SIZE);

    if (parameters.hasPagingState()) {
      builder.pagingState(ByteBuffer.wrap(parameters.getPagingState().getValue().toByteArray()));
    }

    builder.serialConsistencyLevel(
        parameters.hasSerialConsistency()
            ? ConsistencyLevel.fromCode(parameters.getSerialConsistency().getValue().getNumber())
            : BridgeService.DEFAULT_SERIAL_CONSISTENCY);

    if (parameters.hasTimestamp()) {
      builder.defaultTimestamp(parameters.getTimestamp().getValue());
    }

    if (parameters.hasNowInSeconds()) {
      builder.nowInSeconds(parameters.getNowInSeconds().getValue());
    }

    Map<String, ByteBuffer> customPayload = getCustomPayload(clientInfo.orElse(null));
    if (!customPayload.isEmpty()) {
      builder.customPayload(customPayload);
    }

    return builder.tracingRequested(parameters.getTracing()).build();
  }

  private Map<String, ByteBuffer> getCustomPayload(ClientInfo clientInfo) {
    Map<String, ByteBuffer> customPayload = new HashMap<>();

    // add source api if available
    if (null != sourceAPI) {
      sourceAPI.toCustomPayload(customPayload);
    }

    // same for the client info auth data
    if (null != clientInfo) {
      clientInfo.storeAuthenticationData(customPayload);
    }

    return customPayload;
  }
}
