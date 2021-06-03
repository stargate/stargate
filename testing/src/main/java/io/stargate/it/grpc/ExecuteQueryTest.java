package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class ExecuteQueryTest extends GrpcIntegrationTest {

  @Test
  public void simpleQuery(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));
    assertThat(response).isNotNull();
    response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES (?, ?)",
                queryParameters(keyspace),
                Values.of("b"),
                Values.of(2)));
    assertThat(response).isNotNull();

    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getResultSet().getType()).isEqualTo(Payload.Type.CQL);
    ResultSet rs = response.getResultSet().getData().unpack(ResultSet.class);
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    cqlRow(Values.of("a"), Values.of(1)), cqlRow(Values.of("b"), Values.of(2)))));
  }

  @Test
  public void simpleQueryWithPaging() throws InvalidProtocolBufferException {
    Response response =
        stubWithCallCredentials()
            .executeQuery(
                Query.newBuilder()
                    .setCql("select keyspace_name,table_name from system_schema.tables")
                    .setParameters(
                        QueryParameters.newBuilder()
                            .setPageSize(Int32Value.newBuilder().setValue(2).build())
                            .build())
                    .build());

    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getResultSet().getType()).isEqualTo(Payload.Type.CQL);
    ResultSet rs = response.getResultSet().getData().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(2);
    assertThat(rs.getPagingState()).isNotNull();
    assertThat(rs.getPageSize().getValue()).isGreaterThan(0);
  }

  @Test
  public void useKeyspace(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();
    assertThatThrownBy(
            () -> {
              Response response =
                  stub.executeQuery(Query.newBuilder().setCql("USE system").build());
              assertThat(response).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("USE <keyspace> not supported");

    // Verify that system local doesn't work
    assertThatThrownBy(
            () -> {
              Response response =
                  stub.executeQuery(Query.newBuilder().setCql("SELECT * FROM local").build());
              assertThat(response).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("No keyspace has been specified");

    // Verify that setting the keyspace using parameters still works
    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));
    assertThat(response).isNotNull();
  }
}
