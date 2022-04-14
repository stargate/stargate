package io.stargate.sgv2.docsapi.api.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class ErrorCodeRuntimeExceptionMapperTest {
  @Test
  public void testErrorCodeMappings() {
    ErrorCodeRuntimeExceptionMapper mapper = new ErrorCodeRuntimeExceptionMapper();
    for (ErrorCode ec : ErrorCode.values()) {
      ErrorCodeRuntimeException error = new ErrorCodeRuntimeException(ec);
      RestResponse<ApiError> response = mapper.errorCodeRuntimeException(error);
      assertThat(response.getStatus()).isEqualTo(ec.getStatus().getStatusCode());
      assertThat(response.getEntity().description()).isEqualTo(ec.getDefaultMessage());
    }
  }
}
