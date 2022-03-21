package io.stargate.bridge.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.stub.StreamObserver;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.PagingPosition;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Column;
import io.stargate.grpc.service.GrpcService;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class PagingStateHandler {

  public static void makePagingState(
      Schema.MakePagingStateParams params,
      Persistence.Connection connection,
      Persistence persistence,
      StreamObserver<Schema.PagingState> responseObserver) {
    QueryOuterClass.QueryParameters qParams = params.getParameters();
    String keyspace = params.getPosition().getKeyspaceName();
    String decoratedKeyspace =
        persistence.decorateKeyspaceName(keyspace, GrpcService.HEADERS_KEY.get());
    String table = params.getPosition().getTableName();
    Map<String, ByteString> rowMap = params.getPosition().getCurrentRowMap();
    Map<Column, ByteBuffer> currentRow = new HashMap<>();
    List<Column> cols = persistence.schema().keyspace(keyspace).table(table).columns();

    PagingPosition pos =
        new PagingPosition() {
          @Override
          public Map<Column, ByteBuffer> currentRow() {
            for (Map.Entry<String, ByteString> entry : rowMap.entrySet()) {
              Column c =
                  cols.stream().filter(col -> col.name().equals(entry.getKey())).findFirst().get();
              currentRow.put(c, entry.getValue().asReadOnlyByteBuffer());
            }
            return currentRow;
          }

          @Override
          public ResumeMode resumeFrom() {
            return ResumeMode.NEXT_ROW;
          }
        };
    ByteBuffer pagingState =
        connection.makePagingState(
            pos, makeParameters(qParams, decoratedKeyspace, connection.clientInfo()));
    responseObserver.onNext(
        Schema.PagingState.newBuilder()
            .setPagingState(
                BytesValue.newBuilder().setValue(ByteString.copyFrom(pagingState.array())).build())
            .build());
    responseObserver.onCompleted();
  }

  private static Parameters makeParameters(
      QueryOuterClass.QueryParameters parameters,
      String decoratedKeyspace,
      Optional<ClientInfo> clientInfo) {
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
