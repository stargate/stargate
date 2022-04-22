/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.sgv2.docsapi.api.exception;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import org.jboss.resteasy.reactive.RestResponse;

/** {@link RuntimeException} that carries an {@link ErrorCode}. */
public class ErrorCodeRuntimeException extends RuntimeException {

  /** Dedicated {@link ErrorCode}. */
  private final ErrorCode errorCode;

  /** ErrorCode only constructor. */
  public ErrorCodeRuntimeException(ErrorCode errorCode) {
    this(errorCode, errorCode.getDefaultMessage());
  }

  /** @see Exception#Exception(String) */
  public ErrorCodeRuntimeException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  /** @see Exception#Exception(Throwable) */
  public ErrorCodeRuntimeException(ErrorCode errorCode, Throwable cause) {
    super(cause);
    this.errorCode = errorCode;
  }

  /** @see Exception#Exception(String, Throwable) */
  public ErrorCodeRuntimeException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  /** Pipe to {@link ErrorCode#toResponse()} with the exception message. */
  public RestResponse<ApiError> getResponse() {
    return errorCode.toResponse(getMessage());
  }

  /** Pipe to {@link ErrorCode#toResponseBuilder(String)} with the exception message. */
  public RestResponse.ResponseBuilder<ApiError> getResponseBuilder() {
    return errorCode.toResponseBuilder(getMessage());
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
