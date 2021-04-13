package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.stargate.proto.QueryOuterClass.Query;
import org.junit.jupiter.api.Test;

public class AuthenticationTest extends GrpcIntegrationTest {
  @Test
  public void emptyCredentials() {
    assertThatThrownBy(
            () -> {
              stub.execute(Query.newBuilder().setCql("SELECT * FROM system.local").build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED")
        .hasMessageContaining("No token provided");
  }

  @Test
  public void invalidCredentials() {
    assertThatThrownBy(
            () -> {
              stub.withCallCredentials(new StargateBearerToken("not-a-token-that-exists"))
                  .execute(Query.newBuilder().setCql("SELECT * FROM system.local").build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED")
        .hasMessageContaining("Invalid token");
  }
}
