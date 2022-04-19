package io.stargate.sgv2.docsapi.api.exception;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link ConstraintViolationException}. */
public class ConstraintViolationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<ApiError> constraintViolationException(
      ConstraintViolationException exception) {
    return ResponseBuilder.create(
            Response.Status.BAD_REQUEST, new ApiError(exception.getMessage(), 400))
        .build();
  }
}
