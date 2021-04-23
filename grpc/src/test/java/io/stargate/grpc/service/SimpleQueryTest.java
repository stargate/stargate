package io.stargate.grpc.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.grpc.Utils;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class SimpleQueryTest extends BaseServiceTest {
  @Test
  public void simpleQuery() throws InvalidProtocolBufferException {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    Persistence persistence = mock(Persistence.class);
    Connection connection = mock(Connection.class);

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Varchar));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Varchar)));
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              BoundStatement statement =
                  (BoundStatement) invocation.getArgument(0, Statement.class);
              assertThat(statement.preparedId()).isEqualTo(Utils.STATEMENT_ID);
              assertThat(statement.values()).hasSize(1);
              List<List<ByteBuffer>> rows =
                  Arrays.asList(
                      Arrays.asList(
                          TypeCodecs.TEXT.encode(releaseVersion, ProtocolVersion.DEFAULT)));
              return CompletableFuture.completedFuture(new Result.Rows(rows, resultMetadata));
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Result result =
        executeQuery(stub, query, Value.newBuilder().setString("local").build());

    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValues(0).getString()).isEqualTo(releaseVersion);
  }
}
