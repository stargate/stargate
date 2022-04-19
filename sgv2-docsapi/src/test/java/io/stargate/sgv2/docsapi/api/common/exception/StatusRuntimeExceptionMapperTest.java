package io.stargate.sgv2.docsapi.api.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class StatusRuntimeExceptionMapperTest {

  @Test
  public void testGrpcStatusAborted() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.ABORTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.ABORTED.toString());
  }

  @Test
  public void testGrpcStatusAlreadyExists() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.ALREADY_EXISTS);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.ALREADY_EXISTS.toString());
  }

  @Test
  public void testGrpcStatusCancelled() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.CANCELLED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.CANCELLED.toString());
  }

  @Test
  public void testGrpcStatusDataLoss() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.DATA_LOSS);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.DATA_LOSS.toString());
  }

  @Test
  public void testGrpcStatusDeadlineExceeded() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.DEADLINE_EXCEEDED.toString());
  }

  @Test
  public void testGrpcStatusFailedPrecondition() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.FAILED_PRECONDITION);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.FAILED_PRECONDITION.toString());
  }

  @Test
  public void testGrpcStatusInternal() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.INTERNAL);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.INTERNAL.toString());
  }

  @Test
  public void testGrpcStatusInvalidArgument() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.INVALID_ARGUMENT.toString());
  }

  @Test
  public void testGrpcStatusNotFound() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.NOT_FOUND);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.NOT_FOUND.toString());
  }

  @Test
  public void testGrpcStatusOk() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.OK);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.OK.toString());
  }

  @Test
  public void testGrpcStatusOutOfRange() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.OUT_OF_RANGE);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.OUT_OF_RANGE.toString());
  }

  @Test
  public void testGrpcStatusPermissionDenied() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.PERMISSION_DENIED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.PERMISSION_DENIED.toString());
  }

  @Test
  public void testGrpcStatusResourceExhausted() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED.toString());
  }

  @Test
  public void testGrpcStatusUnauthenticated() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNAUTHENTICATED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNAUTHENTICATED.toString());
  }

  @Test
  public void testGrpcStatusUnavailable() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNAVAILABLE);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNAVAILABLE.toString());
  }

  @Test
  public void testGrpcStatusUnimplemented() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNIMPLEMENTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNIMPLEMENTED.toString());
  }

  @Test
  public void testGrpcStatusUnknown() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNKNOWN);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNKNOWN.toString());
  }
}
