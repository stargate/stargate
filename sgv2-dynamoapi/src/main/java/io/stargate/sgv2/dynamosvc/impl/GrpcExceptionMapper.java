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
package io.stargate.sgv2.dynamosvc.impl;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.dynamosvc.models.DynamoServiceError;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class GrpcExceptionMapper implements ExceptionMapper<StatusRuntimeException> {
  @Override
  public Response toResponse(StatusRuntimeException grpcE) {
    Status.Code sc = grpcE.getStatus().getCode();

    // Handling partly based on information from:
    // https://developers.google.com/maps-booking/reference/grpc-api/status_codes
    switch (sc) {
        // Start with codes we know how to handle
      case FAILED_PRECONDITION:
        return grpcResponse(
            sc,
            Response.Status.BAD_REQUEST,
            "Invalid state or argument(s) for gRPC operation",
            grpcE.getMessage());
      case INVALID_ARGUMENT:
        return grpcResponse(
            sc,
            Response.Status.BAD_REQUEST,
            "Invalid argument for gRPC operation",
            grpcE.getMessage());
      case NOT_FOUND:
        return grpcResponse(
            sc, Response.Status.NOT_FOUND, "Unknown gRPC operation", grpcE.getMessage());
      case PERMISSION_DENIED:
        return grpcResponse(
            sc, Response.Status.UNAUTHORIZED, "Unauthorized gRPC operation", grpcE.getMessage());
      case UNAUTHENTICATED:
        return grpcResponse(
            sc, Response.Status.UNAUTHORIZED, "Unauthenticated gRPC operation", grpcE.getMessage());
      case UNAVAILABLE:
        // Should this be SERVICE_UNAVAILABLE (503) or BAD_GATEWAY (502)?
        return grpcResponse(
            sc,
            Response.Status.SERVICE_UNAVAILABLE,
            "gRPC service unavailable",
            grpcE.getMessage());
      case UNIMPLEMENTED:
        return grpcResponse(
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
    return grpcResponse(
        sc, Response.Status.INTERNAL_SERVER_ERROR, "Unhandled gRPC failure", grpcE.getMessage());
  }

  private static Response grpcResponse(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    final String msg = String.format("%s (%s->%s): %s", prefix, grpcCode, httpStatus, suffix);
    return Response.status(httpStatus)
        .entity(new DynamoServiceError(msg, httpStatus.getStatusCode()))
        .build();
  }
}
