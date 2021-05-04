package io.stargate.it.grpc;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.StringValue;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class BatchTest extends GrpcIntegrationTest {

  @Test
  public void simpleBatch(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub tokenStub = stub.withCallCredentials(new StargateBearerToken(authToken));

    tokenStub.executeBatch(
        Batch.newBuilder()
            .addQueries(query("INSERT INTO test (k, v) VALUES ('a', 1)"))
            .addQueries(query("INSERT INTO test (k, v) VALUES ('b', 2)"))
            .addQueries(query("INSERT INTO test (k, v) VALUES ('c', 3)"))
            .setParameters(
                BatchParameters.newBuilder()
                    .setKeyspace(StringValue.of(keyspace.toString()))
                    .build())
            .build());
  }

  private static BatchQuery query(String cql) {
    return BatchQuery.newBuilder().setCql(cql).build();
  }
}
