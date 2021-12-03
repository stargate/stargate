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
package io.stargate.sgv2.restsvc.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sgv2RequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(Sgv2RequestHandler.class);

  /**
   * Method when making the "primary" call, response from which (regardless of being for exception
   * or successful result) will be directly returned from REST endpoint.
   *
   * @param action Call to make to get {@link Response} to return in case no {@code Exception}
   *     caught.
   * @return {@link Response} to return from REST endpoint; may be success or failure.
   */
  public static Response handleMainOperation(Callable<Response> action) {
    try {
      return action.call();
    } catch (StatusRuntimeException grpcE) { // from gRPC
      return failResponseForGrpcException(grpcE);
    } catch (NotFoundException nfe) { // does this actually occur?
      return Response.status(Response.Status.NOT_FOUND)
          .entity(
              new RestServiceError(
                  "Resource not found: " + nfe.getMessage(),
                  Response.Status.NOT_FOUND.getStatusCode()))
          .build();
    } catch (Exception e) {
      return failResponseForOtherException(e);
    }
  }

  private static Response failResponseForGrpcException(StatusRuntimeException grpcE) {
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

  private static Response failResponseForOtherException(Exception e) {
    if (e instanceof IllegalArgumentException) {
      logger.info("Bad request (IllegalArgumentException->BAD_REQUEST): {}", e.getMessage());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new RestServiceError(
                  "Bad request: " + e.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    }
    if (e instanceof JsonProcessingException) {
      logger.info(
          "Invalid JSON payload (JsonProcessingException->BAD_REQUEST): {}", e.getMessage());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new RestServiceError(
                  "Role unauthorized for operation: " + e.getMessage(),
                  Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    }
    if (e instanceof ExecutionException) {
      //      if (ee.getCause() instanceof
      // org.apache.cassandra.stargate.exceptions.UnauthorizedException) { }

      // Do log underlying Exception with Stack trace since this is unknown, unexpected:
      logger.error("Unrecognized error when executing request", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new RestServiceError(
                  "Server error: " + e.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    }
    // Do log underlying Exception with Stack trace since this is unknown, unexpected:
    logger.error("Unrecognized error when executing request", e);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(
            new RestServiceError(
                "Server error: " + e.getMessage(),
                Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
        .build();
  }

  public static Response grpcResponse(
      Status.Code grpcCode, Response.Status httpStatus, String prefix, String suffix) {
    final String msg = String.format("%s (%s->%s): %s", prefix, grpcCode, httpStatus, suffix);
    logger.warn("gRPC returns error response: {}", msg);
    return Response.status(httpStatus)
        .entity(new RestServiceError(msg, httpStatus.getStatusCode()))
        .build();
  }
}
