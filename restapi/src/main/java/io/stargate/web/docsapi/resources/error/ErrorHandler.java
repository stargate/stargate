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

package io.stargate.web.docsapi.resources.error;

import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.models.ApiError;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.exceptions.OverloadedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the exceptions that are thrown on the execution path of the Docs API. */
public final class ErrorHandler
    implements java.util.function.Function<Throwable, Response>,
        io.reactivex.rxjava3.functions.Function<Throwable, Response> {

  private static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);

  public static ErrorHandler EXCEPTION_TO_RESPONSE = new ErrorHandler();

  private ErrorHandler() {}

  @Override
  public Response apply(Throwable throwable) {
    if (throwable instanceof ErrorCodeRuntimeException) {
      ErrorCodeRuntimeException e = (ErrorCodeRuntimeException) throwable;
      return e.getResponse();
    } else if (throwable instanceof UnauthorizedException) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity(
              new ApiError(
                  "Role unauthorized for operation: " + throwable.getMessage(),
                  Response.Status.UNAUTHORIZED.getStatusCode()))
          .build();
    } else if (throwable instanceof OverloadedException) {
      return Response.status(Response.Status.TOO_MANY_REQUESTS)
          .entity(
              new ApiError(
                  "Database is overloaded", Response.Status.TOO_MANY_REQUESTS.getStatusCode()))
          .build();
    } else if (throwable instanceof NoNodeAvailableException) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity(
              new ApiError(
                  "Internal connection to Cassandra closed",
                  Response.Status.SERVICE_UNAVAILABLE.getStatusCode()))
          .build();
    } else {
      logger.error("Error when executing request", throwable);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              new ApiError(
                  "Server error: " + throwable.getLocalizedMessage(),
                  Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
          .build();
    }
  }
}
