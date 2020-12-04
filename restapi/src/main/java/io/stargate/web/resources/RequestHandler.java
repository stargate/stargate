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
import io.stargate.web.models.Error;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces(MediaType.APPLICATION_JSON)
public class RequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

  public static Response handle(Callable<Response> action) {
    try {
      return action.call();
    } catch (NotFoundException nfe) {
      logger.info("Resource not found", nfe);
      return Response.status(Response.Status.NOT_FOUND)
          .entity(
              new Error(
                  "Resource not found: " + nfe.getMessage(),
                  Response.Status.NOT_FOUND.getStatusCode()))
          .build();
    } catch (IllegalArgumentException iae) {
      logger.info("Bad request", iae);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new Error(
                  "Bad request: " + iae.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    } catch (InvalidRequestException ire) {
      logger.info("Bad request", ire);
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new Error(
                  "Bad request: " + ire.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    } catch (UnauthorizedException uae) {
      logger.info("Role unauthorized for operation", uae);
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(
              new Error(
                  "Role unauthorized for operation: " + uae.getMessage(),
                  Response.Status.UNAUTHORIZED.getStatusCode()))
          .build();
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof org.apache.cassandra.stargate.exceptions.UnauthorizedException) {
        logger.info("Role unauthorized for operation", ee);
        return Response.status(Response.Status.UNAUTHORIZED)
            .entity(
                new Error(
                    "Role unauthorized for operation: " + ee.getMessage(),
                    Response.Status.UNAUTHORIZED.getStatusCode()))
            .build();
      } else if (ee.getCause() instanceof InvalidRequestException) {
        logger.info("Bad request", ee);
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(
                new Error(
                    "Bad request: " + ee.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
            .build();
      }

      logger.error("Error when executing request", ee);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new Error(
                  "Server error: " + ee.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    } catch (Exception e) {
      logger.error("Error when executing request", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new Error(
                  "Server error: " + e.getMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    }
  }
}
