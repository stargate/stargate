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
import lombok.extern.slf4j.Slf4j;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

import javax.ws.rs.core.Response;

/**
 * Simple exception mapper for the {@link StatusRuntimeException}.
 */
@Slf4j
public class StatusRuntimeExceptionMapper {

    @ServerExceptionMapper
    public RestResponse<ApiError> statusRuntimeException(StatusRuntimeException exception) {
        log.error("gRPC call failed.", exception);

        Status status = exception.getStatus();
        return switch (status.getCode()) {
            case UNAVAILABLE -> RestResponse.status(Response.Status.BAD_GATEWAY);
            default -> RestResponse.status(Response.Status.INTERNAL_SERVER_ERROR);
        };
    }

}
