package io.stargate.sgv2.docsapi.api.exception;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link RuntimeException}. */
public class RuntimeExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<ApiError> runtimeException(RuntimeException exception) {
    return ResponseBuilder.create(
            Response.Status.INTERNAL_SERVER_ERROR, new ApiError(exception.getMessage(), 500))
        .build();
  }
}
