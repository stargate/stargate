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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;

/**
 * A description of an error state. Note that the external name is {@code Error} (since that is the
 * public entity name in the first release), but internally we use {@code ApiError} to avoid
 * overload with {@link java.lang.Error}.
 */
@ApiModel(value = "Error", description = "A description of an error state")
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ApiError {
  private String description;
  private int code;
  private String internalTxId;

  public ApiError() {
    super();
  }

  public ApiError(String description) {
    this.description = description;
  }

  public ApiError(String description, int code) {
    this.description = description;
    this.code = code;
  }

  /** A human-readable description of the error state */
  public ApiError description(String description) {
    this.description = description;
    return this;
  }

  @ApiModelProperty(
      example =
          "Invalid STRING constant (8be6d514-3436-4e04-a5fc-0ffbefa4c1fe) for \"id\" of type uuid",
      value = "A human readable description of the error state")
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /** The internal number referencing the error state */
  public ApiError code(int code) {
    this.code = code;
    return this;
  }

  @ApiModelProperty(example = "2200", value = "The internal number referencing the error state")
  @JsonProperty("code")
  public int getCode() {
    return code;
  }

  public void setCode(int code) {
    this.code = code;
  }

  /** The internal tracking number of the request */
  public ApiError internalTxId(String internalTxId) {
    this.internalTxId = internalTxId;
    return this;
  }

  @JsonProperty("internalTxId")
  public String getInternalTxId() {
    return internalTxId;
  }

  public void setInternalTxId(String internalTxId) {
    this.internalTxId = internalTxId;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApiError apiError = (ApiError) o;
    return Objects.equals(description, apiError.description)
        && Objects.equals(code, apiError.code)
        && Objects.equals(internalTxId, apiError.internalTxId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, code, internalTxId);
  }

  @Override
  public String toString() {
    return "ApiError{"
        + "description='"
        + description
        + '\''
        + ", code='"
        + code
        + '\''
        + ", internalTxId='"
        + internalTxId
        + '\''
        + '}';
  }
}
