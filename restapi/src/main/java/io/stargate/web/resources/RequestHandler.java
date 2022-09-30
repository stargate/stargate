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
package io.stargate.web.resources;

import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.models.ApiError;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.exceptions.OverloadedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

  public static Response handle(Callable<Response> action) {
    try {
      return action.call();
    } catch (ErrorCodeRuntimeException errorCodeException) {
      return errorCodeException.getResponse();
    } catch (NotFoundException nfe) {
      logger.info("Resource not found (NotFoundException->NOT_FOUND): {}", nfe.getMessage());
      return Response.status(Response.Status.NOT_FOUND)
          .entity(
              new ApiError(
                  "Resource not found: " + nfe.getMessage(),
                  Response.Status.NOT_FOUND.getStatusCode()))
          .build();
    } catch (OverloadedException e) {
      return Response.status(Response.Status.TOO_MANY_REQUESTS)
          .entity(
              new ApiError(
                  "Database is overloaded", Response.Status.TOO_MANY_REQUESTS.getStatusCode()))
          .build();
    } catch (IllegalArgumentException iae) {
      logger.info("Bad request (IllegalArgumentException->BAD_REQUEST): {}", iae.getMessage());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new ApiError(
                  "Bad request: " + iae.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    } catch (InvalidRequestException ire) {
      logger.info("Bad request (InvalidRequestException->BAD_REQUEST): {}", ire.getMessage());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new ApiError(
                  "Bad request: " + ire.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    } catch (UnauthorizedException uae) {
      logger.info(
          "Role unauthorized for operation (UnauthorizedException->UNAUTHORIZED): {}",
          uae.getMessage());
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(
              new ApiError(
                  "Role unauthorized for operation: " + uae.getMessage(),
                  Response.Status.UNAUTHORIZED.getStatusCode()))
          .build();
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof org.apache.cassandra.stargate.exceptions.UnauthorizedException) {
        logger.info(
            "Role unauthorized for operation (ExecutionException/UnauthorizedException->UNAUTHORIZED): {}",
            ee.getMessage());
        return Response.status(Response.Status.UNAUTHORIZED)
            .entity(
                new ApiError(
                    "Role unauthorized for operation: " + ee.getMessage(),
                    Response.Status.UNAUTHORIZED.getStatusCode()))
            .build();
      } else if (ee.getCause() instanceof InvalidRequestException) {
        logger.info(
            "Bad request (ExecutionException/InvalidRequestException->BAD_REQUEST): {}",
            ee.getMessage());
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(
                new ApiError(
                    "Bad request: " + ee.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
            .build();
      }

      // Do log underlying Exception with Stack trace since this is unknown, unexpected:
      logger.error("Internal error when executing request: " + ee, ee);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new ApiError(
                  "Server error: " + ee.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    } catch (Exception e) {
      // Do log underlying Exception with Stack trace since this is unknown, unexpected:
      logger.error("Internal error when executing request: " + e, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new ApiError(
                  "Server error: " + e.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    }
  }
}
