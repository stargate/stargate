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

package io.stargate.sgv2.docsapi.api.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class RuntimeExceptionMapperTest {
  @Test
  public void testErrorCodeMappings() {
    RuntimeExceptionMapper mapper = new RuntimeExceptionMapper();
    RuntimeException error = new RuntimeException("internal error");

    try (RestResponse<ApiError> response = mapper.runtimeException(error)) {
      assertThat(response.getStatus()).isEqualTo(500);
      assertThat(response.getEntity().description()).isEqualTo("internal error");
    }
  }
}
