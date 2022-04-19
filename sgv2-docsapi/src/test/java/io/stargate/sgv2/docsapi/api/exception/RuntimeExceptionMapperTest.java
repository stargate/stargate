package io.stargate.sgv2.docsapi.api.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class RuntimeExceptionMapperTest {
  @Test
  public void testErrorCodeMappings() {
    RuntimeExceptionMapper mapper = new RuntimeExceptionMapper();
    RuntimeException error = new RuntimeException("internal error");
    RestResponse<ApiError> response = mapper.runtimeException(error);
    assertThat(response.getStatus()).isEqualTo(500);
    assertThat(response.getEntity().description()).isEqualTo("internal error");
  }
}
