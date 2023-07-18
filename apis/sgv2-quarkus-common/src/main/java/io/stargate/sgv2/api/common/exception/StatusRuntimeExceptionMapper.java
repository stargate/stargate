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
import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple exception mapper for the {@link StatusRuntimeException}. */
@LookupIfProperty(
    name = "stargate.exception-mappers.enabled",
    stringValue = "true",
    lookupIfMissing = true)
public class StatusRuntimeExceptionMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatusRuntimeExceptionMapper.class);

  @ServerExceptionMapper
  public RestResponse<ApiError> statusRuntimeException(StatusRuntimeException exception) {
    Status.Code sc = exception.getStatus().getCode();
    String msg = exception.getMessage();

    // Handling partly based on information from:
    // https://developers.google.com/maps-booking/reference/grpc-api/status_codes
    switch (sc) {

        // client-side problems, do not log
      case FAILED_PRECONDITION:
        return grpcResponseNoLog(
            sc,
            Response.Status.BAD_REQUEST,
            "Invalid state or argument(s) for gRPC operation",
            msg);
      case INVALID_ARGUMENT:
        return grpcResponseNoLog(
            sc, Response.Status.BAD_REQUEST, "Invalid argument for gRPC operation", msg);
      case NOT_FOUND:
        return grpcResponseNoLog(sc, Response.Status.NOT_FOUND, "Unknown gRPC operation", msg);
      case PERMISSION_DENIED:
        return grpcResponseNoLog(
            sc, Response.Status.UNAUTHORIZED, "Unauthorized gRPC operation", msg);
      case UNAUTHENTICATED:
        return grpcResponseNoLog(
            sc, Response.Status.UNAUTHORIZED, "Unauthenticated gRPC operation", msg);
      case ALREADY_EXISTS:
        return grpcResponseNoLog(sc, Response.Status.CONFLICT, "Resource already exists", msg);

        // server-side problems, do log
      case UNAVAILABLE:
        return grpcResponseAndLogAsError(
            sc, Response.Status.BAD_GATEWAY, "gRPC service unavailable", msg);
      case UNIMPLEMENTED:
        return grpcResponseAndLogAsError(
            sc, Response.Status.NOT_IMPLEMENTED, "Unimplemented gRPC operation", msg);
      case DEADLINE_EXCEEDED:
        return grpcResponseAndLogAsError(
            sc, Response.Status.GATEWAY_TIMEOUT, "gRPC service timeout", msg);

        // and then all codes we consider as not expected
      case OK:
      case CANCELLED:
      case UNKNOWN:
      case RESOURCE_EXHAUSTED:
      case ABORTED:
      case OUT_OF_RANGE:
      case INTERNAL:
      case DATA_LOSS:
        break;
    }

    // Presumed to be server-side issue so log:
    return grpcResponseAndLogAsError(
        sc, Response.Status.INTERNAL_SERVER_ERROR, "Unhandled gRPC failure", msg);
  }

  private static RestResponse<ApiError> grpcResponseAndLogAsError(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    final String message = buildFailureMessage(grpcCode, httpStatus, prefix, suffix);
    LOGGER.error("Bridge gRPC call failed, problem: {}", message);
    return buildRestResponse(grpcCode, httpStatus, message);
  }

  private static RestResponse<ApiError> grpcResponseNoLog(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    return buildRestResponse(
        grpcCode, httpStatus, buildFailureMessage(grpcCode, httpStatus, prefix, suffix));
  }

  private static String buildFailureMessage(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    return String.format(
        "%s (Status.Code.%s->%d): %s", prefix, grpcCode, httpStatus.getStatusCode(), suffix);
  }

  private static RestResponse<ApiError> buildRestResponse(
      Status.Code grpcCode, Response.Status httpStatus, String message) {
    return ResponseBuilder.create(
            httpStatus, new ApiError(message, httpStatus.getStatusCode(), grpcCode.toString()))
        .build();
  }
}
