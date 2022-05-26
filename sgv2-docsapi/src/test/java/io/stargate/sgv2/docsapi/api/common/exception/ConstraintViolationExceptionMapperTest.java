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

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import java.util.HashSet;
import javax.validation.ConstraintViolationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class ConstraintViolationExceptionMapperTest {
  @Test
  public void testErrorCodeMappings() {
    ConstraintViolationExceptionMapper mapper = new ConstraintViolationExceptionMapper();
    ConstraintViolationException error =
        new ConstraintViolationException("Email must not be empty", new HashSet<>());

    try (RestResponse<ApiError> response = mapper.constraintViolationException(error)) {
      assertThat(response.getStatus()).isEqualTo(400);
      assertThat(response.getEntity().description()).isEqualTo("Email must not be empty");
    }
  }
}
