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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.api.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.api.common.grpc.BridgeAuthorizationException;
import io.stargate.sgv2.api.common.grpc.UnauthorizedKeyspaceException;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

class BridgeAuthorizationExceptionMapperTest {

  @Test
  public void happyPath() {
    BridgeAuthorizationExceptionMapper mapper = new BridgeAuthorizationExceptionMapper();
    BridgeAuthorizationException error = new UnauthorizedKeyspaceException("my_keyspace");

    try (RestResponse<ApiError> response = mapper.bridgeAuthorizationException(error)) {
      assertThat(response.getStatus()).isEqualTo(401);
      assertThat(response.getEntity().description())
          .isEqualTo("Unauthorized keyspace: my_keyspace");
    }
  }
}
