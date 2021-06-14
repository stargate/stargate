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
package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.StatusRuntimeException;
import io.stargate.proto.QueryOuterClass.Query;
import org.junit.jupiter.api.Test;

public class AuthenticationTest extends GrpcIntegrationTest {
  @Test
  public void emptyCredentials() {
    assertThatThrownBy(
            () -> {
              stub.executeQuery(Query.newBuilder().setCql("SELECT * FROM system.local").build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED")
        .hasMessageContaining("No token provided");
  }

  @Test
  public void invalidCredentials() {
    assertThatThrownBy(
            () -> {
              stubWithCallCredentials("not-a-token-that-exists")
                  .executeQuery(Query.newBuilder().setCql("SELECT * FROM system.local").build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED")
        .hasMessageContaining("Invalid token");
  }
}
