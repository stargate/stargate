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

package io.stargate.sgv2.dynamosvc.impl;

import io.dropwizard.jersey.validation.ConstraintMessage;
import io.dropwizard.jersey.validation.JerseyViolationException;
import io.stargate.sgv2.dynamosvc.models.DynamoServiceError;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.glassfish.jersey.server.model.Invocable;

/**
 * Maps {@link JerseyViolationException}s to the response with the {@link DynamoServiceError}
 * entity.
 */
public class JerseyViolationExceptionMapper implements ExceptionMapper<JerseyViolationException> {

  /** {@inheritDoc} */
  @Override
  public Response toResponse(JerseyViolationException exception) {
    Set<ConstraintViolation<?>> violations = exception.getConstraintViolations();
    Invocable invocable = exception.getInvocable();

    String violationMessages =
        violations.stream()
            .map(ConstraintViolation::getMessage)
            .distinct()
            .collect(Collectors.joining(", "));
    String message = String.format("Request invalid: %s.", violationMessages);
    int status = ConstraintMessage.determineStatus(violations, invocable);

    DynamoServiceError error = new DynamoServiceError(message, status);
    return Response.status(status).type(MediaType.APPLICATION_JSON).entity(error).build();
  }
}
