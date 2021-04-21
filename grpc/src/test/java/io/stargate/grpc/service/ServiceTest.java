package io.stargate.grpc.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Flag;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.PreparedMetadata;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServiceTest {
  private static final String SERVER_NAME = "ServiceTests";
  private static final MD5Digest RESULT_METADATA_ID = MD5Digest.compute("resultMetadata");
  private static final MD5Digest STATEMENT_ID = MD5Digest.compute("statement");
  private static final EnumSet EMPTY_FLAGS = EnumSet.noneOf(Flag.class);

  private Server server;

  @BeforeEach
  public void setup() {}

  @AfterEach
  void cleanUp() {
    try {
      if (server != null) {
        server.shutdown().awaitTermination();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void simpleQuery() throws InvalidProtocolBufferException {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    Persistence persistence = mock(Persistence.class);
    Connection connection = mock(Connection.class);

    ResultMetadata resultMetadata =
        makeResultMetadata(Column.create("release_version", Type.Varchar));
    Prepared prepared =
        new Prepared(
            STATEMENT_ID,
            RESULT_METADATA_ID,
            resultMetadata,
            makePreparedMetadata(Column.create("key", Type.Varchar)));
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              BoundStatement statement =
                  (BoundStatement) invocation.getArgument(0, Statement.class);
              assertThat(statement.preparedId()).isEqualTo(STATEMENT_ID);
              assertThat(statement.values()).hasSize(1);
              List<List<ByteBuffer>> rows =
                  Arrays.asList(
                      Arrays.asList(
                          TypeCodecs.TEXT.encode(releaseVersion, ProtocolVersion.DEFAULT)));
              return CompletableFuture.completedFuture(new Result.Rows(rows, resultMetadata));
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    ManagedChannel channel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    StargateBlockingStub stub = StargateGrpc.newBlockingStub(channel);

    QueryOuterClass.Result result =
        executeQuery(stub, query, Value.newBuilder().setString("local").build());

    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValues(0).getString()).isEqualTo(releaseVersion);
  }

  private QueryOuterClass.Result executeQuery(
      StargateBlockingStub stub, String cql, Value... values) {
    return stub.executeQuery(
        Query.newBuilder()
            .setCql(cql)
            .setParameters(
                QueryParameters.newBuilder()
                    .setPayload(
                        Payload.newBuilder()
                            .setType(Payload.Type.TYPE_CQL)
                            .setValue(
                                Any.pack(
                                    Values.newBuilder()
                                        .addAllValues(Arrays.asList(values))
                                        .build()))
                            .build())
                    .build())
            .build());
  }

  private void startServer(Persistence persistence) {
    server =
        InProcessServerBuilder.forName(SERVER_NAME)
            .directExecutor()
            .intercept(new MockInterceptor())
            .addService(new Service(persistence, mock(Metrics.class)))
            .build();
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ResultMetadata makeResultMetadata(Column... columns) {
    return new ResultMetadata(
        EMPTY_FLAGS, columns.length, Arrays.asList(columns), RESULT_METADATA_ID, null);
  }

  private PreparedMetadata makePreparedMetadata(Column... columns) {
    return new PreparedMetadata(EMPTY_FLAGS, Arrays.asList(columns), null);
  }
}
