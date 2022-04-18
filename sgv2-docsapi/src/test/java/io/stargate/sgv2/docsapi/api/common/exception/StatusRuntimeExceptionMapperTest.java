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
  public void testGrpcStatusAborted() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.ABORTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)", Status.Code.ABORTED, getResponseStatus(Status.Code.ABORTED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusAlreadyExists() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.ALREADY_EXISTS);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.ALREADY_EXISTS, getResponseStatus(Status.Code.ALREADY_EXISTS))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusCancelled() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.CANCELLED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.CANCELLED, getResponseStatus(Status.Code.CANCELLED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusDataLoss() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.DATA_LOSS);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.DATA_LOSS, getResponseStatus(Status.Code.DATA_LOSS))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusDeadlineExceeded() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.DEADLINE_EXCEEDED,
                        getResponseStatus(Status.Code.DEADLINE_EXCEEDED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusFailedPrecondition() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.FAILED_PRECONDITION);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.FAILED_PRECONDITION,
                        getResponseStatus(Status.Code.FAILED_PRECONDITION))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusInternal() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.INTERNAL);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)", Status.Code.INTERNAL, getResponseStatus(Status.Code.INTERNAL))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusInvalidArgument() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.INVALID_ARGUMENT,
                        getResponseStatus(Status.Code.INVALID_ARGUMENT))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusNotFound() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.NOT_FOUND);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.NOT_FOUND, getResponseStatus(Status.Code.NOT_FOUND))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusOk() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.OK);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format("(%s->%s)", Status.Code.OK, getResponseStatus(Status.Code.OK))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusOutOfRange() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.OUT_OF_RANGE);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.OUT_OF_RANGE, getResponseStatus(Status.Code.OUT_OF_RANGE))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusPermissionDenied() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.PERMISSION_DENIED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.PERMISSION_DENIED,
                        getResponseStatus(Status.Code.PERMISSION_DENIED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusResourceExhausted() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.RESOURCE_EXHAUSTED,
                        getResponseStatus(Status.Code.RESOURCE_EXHAUSTED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusUnauthenticated() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNAUTHENTICATED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.UNAUTHENTICATED,
                        getResponseStatus(Status.Code.UNAUTHENTICATED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusUnavailable() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNAVAILABLE);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.UNAVAILABLE, getResponseStatus(Status.Code.UNAVAILABLE))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusUnimplemented() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNIMPLEMENTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)",
                        Status.Code.UNIMPLEMENTED, getResponseStatus(Status.Code.UNIMPLEMENTED))))
        .isTrue();
  }

  @Test
  public void testGrpcStatusUnknown() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNKNOWN);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(
            response
                .getEntity()
                .description()
                .contains(
                    String.format(
                        "(%s->%s)", Status.Code.UNKNOWN, getResponseStatus(Status.Code.UNKNOWN))))
        .isTrue();
  }
}
