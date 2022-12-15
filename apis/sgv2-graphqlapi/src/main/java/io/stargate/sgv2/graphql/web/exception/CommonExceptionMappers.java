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

package io.stargate.sgv2.graphql.web.exception;

import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.api.common.exception.BridgeAuthorizationExceptionMapper;
import io.stargate.sgv2.api.common.exception.ConstraintViolationExceptionMapper;
import io.stargate.sgv2.api.common.exception.RuntimeExceptionMapper;
import io.stargate.sgv2.api.common.exception.StatusRuntimeExceptionMapper;
import io.stargate.sgv2.api.common.exception.WebApplicationExceptionMapper;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.api.common.grpc.BridgeAuthorizationException;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * A setup for activating all common exceptions mappers. We have this class in order to have any
 * exception mapper in the commons optional. Unfortunately, this same setup exists in all three
 * APIs.
 */
public class CommonExceptionMappers {

  private final BridgeAuthorizationExceptionMapper bridgeAuthorizationExceptionMapper =
      new BridgeAuthorizationExceptionMapper();

  private final ConstraintViolationExceptionMapper constraintViolationExceptionMapper =
      new ConstraintViolationExceptionMapper();

  private final RuntimeExceptionMapper runtimeExceptionMapper = new RuntimeExceptionMapper();

  private final StatusRuntimeExceptionMapper statusRuntimeExceptionMapper =
      new StatusRuntimeExceptionMapper();

  private final WebApplicationExceptionMapper webApplicationExceptionMapper =
      new WebApplicationExceptionMapper();

  @ServerExceptionMapper
  public RestResponse<ApiError> bridgeAuthorizationException(
      BridgeAuthorizationException exception) {
    return bridgeAuthorizationExceptionMapper.bridgeAuthorizationException(exception);
  }

  @ServerExceptionMapper
  public RestResponse<ApiError> constraintViolationException(
      ConstraintViolationException exception) {
    return constraintViolationExceptionMapper.constraintViolationException(exception);
  }

  @ServerExceptionMapper
  public RestResponse<ApiError> runtimeException(RuntimeException exception) {
    return runtimeExceptionMapper.runtimeException(exception);
  }

  @ServerExceptionMapper
  public RestResponse<ApiError> statusRuntimeException(StatusRuntimeException exception) {
    return statusRuntimeExceptionMapper.statusRuntimeException(exception);
  }

  @ServerExceptionMapper
  public Response webApplicationException(WebApplicationException exception) {
    return webApplicationExceptionMapper.webApplicationException(exception);
  }
}
