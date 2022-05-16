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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.restsvc.impl;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ExceptionMapper used to deal with gRPC problems when accessing Bridge gRPC service. */
public class GrpcExceptionMapper implements ExceptionMapper<StatusRuntimeException> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcExceptionMapper.class);

  @Override
  public Response toResponse(StatusRuntimeException grpcE) {
    Status.Code sc = grpcE.getStatus().getCode();

    // Handling partly based on information from:
    // https://developers.google.com/maps-booking/reference/grpc-api/status_codes

    // 16-May-2022, tatu: As per [stargate#1818] server-side failures should
    //    be logged (since they are not due to caller), but client-side failures not.
    //    This is meant to help operational support, troubleshooting, without clogging
    //    logs.
    switch (sc) {
      case FAILED_PRECONDITION: // client problem, don't log
        return grpcResponse(
            sc,
            Response.Status.BAD_REQUEST, // client problem, don't log
            "Invalid state or argument(s) for gRPC operation",
            grpcE.getMessage());
      case INVALID_ARGUMENT: // client problem, don't log
        return grpcResponse(
            sc,
            Response.Status.BAD_REQUEST,
            "Invalid argument for gRPC operation",
            grpcE.getMessage());
      case NOT_FOUND: // probably client problem, don't log
        return grpcResponse(
            sc, Response.Status.NOT_FOUND, "Unknown gRPC operation", grpcE.getMessage());
      case PERMISSION_DENIED: // client problem, don't log
        return grpcResponse(
            sc, Response.Status.UNAUTHORIZED, "Unauthorized gRPC operation", grpcE.getMessage());
      case UNAUTHENTICATED: // client problem, don't log
        return grpcResponse(
            sc, Response.Status.UNAUTHORIZED, "Unauthenticated gRPC operation", grpcE.getMessage());
      case UNAVAILABLE: // server-side problem, do log
        return grpcResponseAndLog(
            sc,
            Response.Status.SERVICE_UNAVAILABLE,
            "gRPC service unavailable",
            grpcE.getMessage());
      case UNIMPLEMENTED: // server-side problem, do log
        return grpcResponseAndLog(
            sc,
            Response.Status.NOT_IMPLEMENTED,
            "Unimplemented gRPC operation",
            grpcE.getMessage());

        // And then codes we ... don't know how to handle. Assume server-side
        // so do log
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
    return grpcResponseAndLog(
        sc, Response.Status.INTERNAL_SERVER_ERROR, "Unhandled gRPC failure", grpcE.getMessage());
  }

  private static Response grpcResponseAndLog(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    final String message = buildResponseMessage(grpcCode, httpStatus, prefix, suffix);
    LOGGER.error("Problem with Bridge gRPC service: {}", message);
    return buildGrpcResponse(httpStatus, message);
  }

  private static Response grpcResponse(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    return buildGrpcResponse(
        httpStatus, buildResponseMessage(grpcCode, httpStatus, prefix, suffix));
  }

  private static String buildResponseMessage(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    return String.format("%s (%s->%s): %s", prefix, grpcCode, httpStatus, suffix);
  }

  private static Response buildGrpcResponse(Response.Status httpStatus, String message) {
    return Response.status(httpStatus)
        .entity(new RestServiceError(message, httpStatus.getStatusCode()))
        .build();
  }
}
