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

package io.stargate.sgv2.docsapi.api.common.exception;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * Simple exception mapper for the {@link WebApplicationException}. Needed due to the existence of
 * the {@link RuntimeExceptionMapper} that must not be used for these exceptions.
 */
public class WebApplicationExceptionMapper {

  @ServerExceptionMapper
  public Response webApplicationException(WebApplicationException exception) {
    Response response = exception.getResponse();
    ApiError error = new ApiError(exception.getMessage(), response.getStatus());
    return Response.fromResponse(response).entity(error).build();
  }
}
