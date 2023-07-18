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
package io.stargate.sgv2.api.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Test;

public class StatusRuntimeExceptionMapperTest {

  @Test
  public void grpcStatusAborted() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.ABORTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.ABORTED.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusAlreadyExists() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.ALREADY_EXISTS);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.ALREADY_EXISTS.toString());
    assertThat(response.getEntity().code()).isEqualTo(409);
  }

  @Test
  public void grpcStatusCancelled() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.CANCELLED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.CANCELLED.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusDataLoss() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.DATA_LOSS);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.DATA_LOSS.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusDeadlineExceeded() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.DEADLINE_EXCEEDED.toString());
    assertThat(response.getEntity().code()).isEqualTo(504);
  }

  @Test
  public void grpcStatusFailedPrecondition() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.FAILED_PRECONDITION);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.FAILED_PRECONDITION.toString());
    assertThat(response.getEntity().code()).isEqualTo(400);
  }

  @Test
  public void grpcStatusInternal() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.INTERNAL);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.INTERNAL.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusInvalidArgument() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.INVALID_ARGUMENT.toString());
    assertThat(response.getEntity().code()).isEqualTo(400);
  }

  @Test
  public void grpcStatusNotFound() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.NOT_FOUND);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.NOT_FOUND.toString());
    assertThat(response.getEntity().code()).isEqualTo(404);
  }

  @Test
  public void grpcStatusOk() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.OK);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.OK.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusOutOfRange() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.OUT_OF_RANGE);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.OUT_OF_RANGE.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusPermissionDenied() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.PERMISSION_DENIED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.PERMISSION_DENIED.toString());
    assertThat(response.getEntity().code()).isEqualTo(401);
  }

  @Test
  public void grpcStatusResourceExhausted() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }

  @Test
  public void grpcStatusUnauthenticated() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNAUTHENTICATED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNAUTHENTICATED.toString());
    assertThat(response.getEntity().code()).isEqualTo(401);
  }

  @Test
  public void grpcStatusUnavailable() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNAVAILABLE);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNAVAILABLE.toString());
    assertThat(response.getEntity().code()).isEqualTo(502);
  }

  @Test
  public void grpcStatusUnimplemented() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNIMPLEMENTED);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNIMPLEMENTED.toString());
    assertThat(response.getEntity().code()).isEqualTo(501);
  }

  @Test
  public void grpcStatusUnknown() {
    StatusRuntimeExceptionMapper mapper = new StatusRuntimeExceptionMapper();
    StatusRuntimeException error = new StatusRuntimeException(Status.UNKNOWN);
    RestResponse<ApiError> response = mapper.statusRuntimeException(error);
    assertThat(response.getEntity().description().endsWith(error.getMessage())).isTrue();
    assertThat(response.getEntity().grpcStatus()).isEqualTo(Status.Code.UNKNOWN.toString());
    assertThat(response.getEntity().code()).isEqualTo(500);
  }
}
