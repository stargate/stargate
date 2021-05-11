package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Result;
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
    StargateBlockingStub stub = this.stub.withCallCredentials(new StargateBearerToken(authToken));

    StringValue keyspaceValue = StringValue.of(keyspace.toString());

    Result result =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES ('a', 1)",
                cqlQueryParameters().setKeyspace(keyspaceValue)));
    assertThat(result).isNotNull();
    result =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES (?, ?)",
                cqlQueryParameters(stringValue("b"), intValue(2)).setKeyspace(keyspaceValue)));
    assertThat(result).isNotNull();

    result =
        stub.executeQuery(
            cqlQuery("SELECT * FROM test", cqlQueryParameters().setKeyspace(keyspaceValue)));
    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    cqlRow(stringValue("a"), intValue(1)), cqlRow(stringValue("b"), intValue(2)))));
  }

  @Test
  public void simpleQueryWithPaging() throws InvalidProtocolBufferException {
    Result result =
        stub.withCallCredentials(new StargateBearerToken(authToken))
            .executeQuery(
                Query.newBuilder()
                    .setCql("select keyspace_name,table_name from system_schema.tables")
                    .setParameters(
                        QueryParameters.newBuilder()
                            .setPayload(Payload.newBuilder().setType(Type.TYPE_CQL).build())
                            .setPageSize(Int32Value.newBuilder().setValue(2).build())
                            .build())
                    .build());

    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(2);
    assertThat(rs.getPagingState()).isNotNull();
    assertThat(rs.getPageSize().getValue()).isGreaterThan(0);
  }
}
