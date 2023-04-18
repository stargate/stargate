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

import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.core.Response;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link ConstraintViolationException}. */
@LookupIfProperty(
    name = "stargate.exception-mappers.enabled",
    stringValue = "true",
    lookupIfMissing = true)
public class ConstraintViolationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<ApiError> constraintViolationException(
      ConstraintViolationException exception) {
    // figure out the message in the same way as V1
    Set<ConstraintViolation<?>> violations = exception.getConstraintViolations();
    String violationMessages =
        violations.stream()
            .map(ConstraintViolation::getMessage)
            .distinct()
            .collect(Collectors.joining(", "));
    String message = String.format("Request invalid: %s.", violationMessages);

    int code = Response.Status.BAD_REQUEST.getStatusCode();
    ApiError error = new ApiError(message, code);
    return ResponseBuilder.create(Response.Status.BAD_REQUEST, error).build();
  }
}
