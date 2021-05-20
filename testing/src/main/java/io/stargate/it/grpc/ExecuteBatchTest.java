package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.BatchParameters;
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
public class ExecuteBatchTest extends GrpcIntegrationTest {

  @Test
  public void simpleBatch(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    StringValue keyspaceValue = StringValue.of(keyspace.toString());

    Result result =
        stub.executeBatch(
            Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .setParameters(BatchParameters.newBuilder().setKeyspace(keyspaceValue).build())
                .build());
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
                    cqlRow(Values.of("a"), Values.of(1)),
                    cqlRow(Values.of("b"), Values.of(2)),
                    cqlRow(Values.of("c"), Values.of(3)))));
  }
}
