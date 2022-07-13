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

import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple exception mapper for the {@link RuntimeException}. */
public class RuntimeExceptionMapper {

  private static final Logger log = LoggerFactory.getLogger(RuntimeExceptionMapper.class);

  @ServerExceptionMapper
  public RestResponse<ApiError> runtimeException(RuntimeException exception) {
    log.error("Unexpected runtime exception occurred.", exception);

    int code = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
    ApiError error = new ApiError(exception.getMessage(), code);
    return ResponseBuilder.create(Response.Status.INTERNAL_SERVER_ERROR, error).build();
  }
}
