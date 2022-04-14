package io.stargate.sgv2.docsapi.api.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class StatusRuntimeExceptionMapperTest {

  private Response.Status getResponseStatus(Status.Code code) {
    switch (code) {
      case FAILED_PRECONDITION:
      case INVALID_ARGUMENT:
        return Response.Status.BAD_REQUEST;
      case NOT_FOUND:
        return Response.Status.NOT_FOUND;
      case PERMISSION_DENIED:
      case UNAUTHENTICATED:
        return Response.Status.UNAUTHORIZED;
      case UNAVAILABLE:
        return Response.Status.SERVICE_UNAVAILABLE;
      case UNIMPLEMENTED:
        return Response.Status.NOT_IMPLEMENTED;
      default:
        return Response.Status.INTERNAL_SERVER_ERROR;
    }
  }

  @Test
  public void testErrorCodeMappings() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    for (Status.Code code : Status.Code.values()) {
      StatusRuntimeException error = new StatusRuntimeException(Status.fromCode(code));
      RestResponse<ApiError> response = mapper.statusRuntimeException(error);
      assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
      assertThat(
              response
                  .getEntity()
                  .description()
                  .contains(String.format("(%s->%s)", code, getResponseStatus(code))))
          .isTrue();
    }
  }
}
