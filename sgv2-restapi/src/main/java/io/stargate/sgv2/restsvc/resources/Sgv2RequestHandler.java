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
import io.stargate.sgv2.restsvc.models.RestServiceError;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sgv2RequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(Sgv2RequestHandler.class);

  public static Response handle(Callable<Response> action) {
    try {
      return action.call();
    } catch (NotFoundException nfe) {
      logger.info("Resource not found (NotFoundException->NOT_FOUND): {}", nfe.getMessage());
      return Response.status(Response.Status.NOT_FOUND)
          .entity(
              new RestServiceError(
                  "Resource not found: " + nfe.getMessage(),
                  Response.Status.NOT_FOUND.getStatusCode()))
          .build();
    } catch (IllegalArgumentException iae) {
      logger.info("Bad request (IllegalArgumentException->BAD_REQUEST): {}", iae.getMessage());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new RestServiceError(
                  "Bad request: " + iae.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    } catch (JsonProcessingException jpe) {
      logger.info(
          "Invalid JSON payload (JsonProcessingException->BAD_REQUEST): {}", jpe.getMessage());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new RestServiceError(
                  "Role unauthorized for operation: " + jpe.getMessage(),
                  Response.Status.UNAUTHORIZED.getStatusCode()))
          .build();
    } catch (ExecutionException ee) {
      //      if (ee.getCause() instanceof
      // org.apache.cassandra.stargate.exceptions.UnauthorizedException) { }

      // Do log underlying Exception with Stack trace since this is unknown, unexpected:
      logger.error("Error when executing request", ee);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new RestServiceError(
                  "Server error: " + ee.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    } catch (Exception e) {
      // Do log underlying Exception with Stack trace since this is unknown, unexpected:
      logger.error("Error when executing request", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new RestServiceError(
                  "Server error: " + e.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    }
  }
}
