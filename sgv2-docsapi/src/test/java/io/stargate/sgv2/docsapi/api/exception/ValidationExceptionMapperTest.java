package io.stargate.sgv2.docsapi.api.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.validation.ValidationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class ValidationExceptionMapperTest {
  @Test
  public void testErrorCodeMappings() {
    ValidationExceptionMapper mapper = new ValidationExceptionMapper();
    ValidationException error = new ValidationException("Email must not be empty");
    RestResponse<ApiError> response = mapper.validationException(error);
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getEntity().description()).isEqualTo("Email must not be empty");
  }
}
