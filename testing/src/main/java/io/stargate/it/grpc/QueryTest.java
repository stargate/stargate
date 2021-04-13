package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Result;
import org.junit.jupiter.api.Test;

public class QueryTest extends GrpcIntegrationTest {
  @Test
  public void simpleQuery() {
    Result result =
        stub.withCallCredentials(new StargateBearerToken(authToken))
            .execute(Query.newBuilder().setCql("SELECT * FROM system.local").build());
    // TODO: Get and check a "real" result
    assertThat(result.hasError()).isFalse();
    assertThat(result.hasEmpty()).isTrue();
  }
}
