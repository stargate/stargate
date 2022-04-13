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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Simple exception mapper for the {@link StatusRuntimeException}. */
public class StatusRuntimeExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<ApiError> statusRuntimeException(StatusRuntimeException exception) {
    // TODO finalize mapper, messages, logs, etc.

    Status status = exception.getStatus();
    ApiError body = new ApiError(status.getDescription());

    return switch (status.getCode()) {
      case UNAVAILABLE -> RestResponse.status(Response.Status.BAD_GATEWAY, body);
      case UNAUTHENTICATED -> RestResponse.status(Response.Status.UNAUTHORIZED, body);
      default -> RestResponse.status(Response.Status.INTERNAL_SERVER_ERROR, body);
    };
  }
}
