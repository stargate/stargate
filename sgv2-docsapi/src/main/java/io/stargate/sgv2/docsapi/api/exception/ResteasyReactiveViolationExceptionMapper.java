package io.stargate.sgv2.docsapi.api.exception;

import io.quarkus.hibernate.validator.runtime.jaxrs.ResteasyReactiveViolationException;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link ResteasyReactiveViolationException}. */
public class ResteasyReactiveViolationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<ApiError> resteasyReactiveViolationException(
      ResteasyReactiveViolationException exception) {
    return ResponseBuilder.create(
            Response.Status.BAD_REQUEST, new ApiError(exception.getMessage(), 400))
        .build();
  }
}
