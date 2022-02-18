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

import io.stargate.sgv2.dynamosvc.models.DynamoServiceError;
import java.util.concurrent.ThreadLocalRandom;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class DefaultExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultExceptionMapper.class);

  @Override
  public Response toResponse(Throwable exception) {
    // If we're dealing with a web exception, we can service certain types of request (like
    // redirection or server errors) better and also propagate properties of the inner response.
    if (exception instanceof WebApplicationException) {
      final Response response = ((WebApplicationException) exception).getResponse();
      Response.Status.Family family = response.getStatusInfo().getFamily();
      if (family.equals(Response.Status.Family.REDIRECTION)) {
        return response;
      } else if (family.equals(Response.Status.Family.SERVER_ERROR)) {
        return handleUnexpected(exception);
      } else {
        return Response.fromResponse(response)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(new DynamoServiceError(exception.getLocalizedMessage(), response.getStatus()))
            .build();
      }
    } else {
      return handleUnexpected(exception);
    }
  }

  // The thrown exception is a not a web exception, so the exception is most likely unexpected.
  // We'll create a unique id in the server error response that is also logged for correlation
  private Response handleUnexpected(Throwable exception) {
    String uniqueId = Long.toHexString(ThreadLocalRandom.current().nextLong());
    LOGGER.error("Error handling a request: {}", uniqueId, exception);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            new DynamoServiceError(
                    String.format(
                        "There was an error processing your request. It has been logged (ID %s).",
                        uniqueId),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
                .internalTxId(uniqueId))
        .build();
  }
}
