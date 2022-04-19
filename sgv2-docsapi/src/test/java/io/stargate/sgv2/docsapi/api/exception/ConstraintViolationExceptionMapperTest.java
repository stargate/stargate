package io.stargate.sgv2.docsapi.api.exception;

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
    RestResponse<ApiError> response = mapper.constraintViolationException(error);
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getEntity().description()).isEqualTo("Email must not be empty");
  }
}
