package io.stargate.sgv2.docsapi.api.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.hibernate.validator.runtime.jaxrs.ResteasyReactiveViolationException;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import java.util.HashSet;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class ResteasyReactiveViolationExceptionMapperTest {
  @Test
  public void testErrorCodeMappings() {
    ResteasyReactiveViolationExceptionMapper mapper =
        new ResteasyReactiveViolationExceptionMapper();
    ResteasyReactiveViolationException error =
        new ResteasyReactiveViolationException("Email must not be empty", new HashSet<>());
    RestResponse<ApiError> response = mapper.resteasyReactiveViolationException(error);
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getEntity().description()).isEqualTo("Email must not be empty");
  }
}
