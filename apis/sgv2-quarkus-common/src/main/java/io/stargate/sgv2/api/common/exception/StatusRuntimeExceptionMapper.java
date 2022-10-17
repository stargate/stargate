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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple exception mapper for the {@link StatusRuntimeException}. */
public class StatusRuntimeExceptionMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatusRuntimeExceptionMapper.class);

  @ServerExceptionMapper
  public RestResponse<ApiError> statusRuntimeException(StatusRuntimeException grpcE) {
    // TODO finalize mapper, messages, logs, etc.

    Status.Code sc = grpcE.getStatus().getCode();

    // Handling partly based on information from:
    // https://developers.google.com/maps-booking/reference/grpc-api/status_codes
    switch (sc) {
        // Start with codes we know how to handle
      case FAILED_PRECONDITION:
        return grpcResponseNoLog(
            sc,
            Response.Status.BAD_REQUEST,
            "Invalid state or argument(s) for gRPC operation",
            grpcE.getMessage());
      case INVALID_ARGUMENT:
        return grpcResponseNoLog(
            sc,
            Response.Status.BAD_REQUEST,
            "Invalid argument for gRPC operation",
            grpcE.getMessage());
      case NOT_FOUND:
        return grpcResponseNoLog(
            sc, Response.Status.NOT_FOUND, "Unknown gRPC operation", grpcE.getMessage());
      case PERMISSION_DENIED:
        return grpcResponseNoLog(
            sc, Response.Status.UNAUTHORIZED, "Unauthorized gRPC operation", grpcE.getMessage());
      case UNAUTHENTICATED:
        return grpcResponseNoLog(
            sc, Response.Status.UNAUTHORIZED, "Unauthenticated gRPC operation", grpcE.getMessage());
      case UNAVAILABLE: // server-side problem, do log
        // Should this be SERVICE_UNAVAILABLE (503) or BAD_GATEWAY (502)?
        return grpcResponseAndLogAsError(
            sc,
            Response.Status.SERVICE_UNAVAILABLE,
            "gRPC service unavailable",
            grpcE.getMessage());
      case UNIMPLEMENTED: // server-side problem, do log
        return grpcResponseAndLogAsError(
            sc,
            Response.Status.NOT_IMPLEMENTED,
            "Unimplemented gRPC operation",
            grpcE.getMessage());

        // And then codes we ... don't know how to handle
      case OK: // huh?

      case CANCELLED:
      case UNKNOWN:
      case DEADLINE_EXCEEDED:
      case ALREADY_EXISTS:
      case RESOURCE_EXHAUSTED:
      case ABORTED:
      case OUT_OF_RANGE:
      case INTERNAL:
      case DATA_LOSS:
        break;
    }

    // Presumed to be server-side issue so log:
    return grpcResponseAndLogAsError(
        sc, Response.Status.INTERNAL_SERVER_ERROR, "Unhandled gRPC failure", grpcE.getMessage());
  }

  private static RestResponse<ApiError> grpcResponseAndLogAsError(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    final String message = buildFailureMessage(grpcCode, httpStatus, prefix, suffix);
    LOGGER.error("Bridge gRPC call failed, problem: {}", message);
    return buildGrpcResponse(grpcCode, httpStatus, message);
  }

  private static RestResponse<ApiError> grpcResponseNoLog(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    return buildGrpcResponse(
        grpcCode, httpStatus, buildFailureMessage(grpcCode, httpStatus, prefix, suffix));
  }

  private static String buildFailureMessage(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    return String.format(
        "%s (Status.Code.%s->%d): %s", prefix, grpcCode, httpStatus.getStatusCode(), suffix);
  }

  private static RestResponse<ApiError> buildGrpcResponse(
      Status.Code grpcCode, Response.Status httpStatus, String message) {
    return ResponseBuilder.create(
            httpStatus, new ApiError(message, httpStatus.getStatusCode(), grpcCode.toString()))
        .build();
  }
}
