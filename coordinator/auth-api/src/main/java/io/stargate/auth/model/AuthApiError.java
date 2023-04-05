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
package io.stargate.auth.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;

/**
 * A description of an error state. Note: external name is {@code Error} for backwards-compatibility
 * reasons.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@ApiModel(value = "Error", description = "A description of an error state")
public final class AuthApiError {
  private String description;
  private String internalCode;
  private String internalTxId;

  AuthApiError() {
    super();
  }

  public AuthApiError(String description) {
    this.description = description;
  }

  /** A human readable description of the error state */
  public AuthApiError description(String description) {
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
  public AuthApiError internalCode(String internalCode) {
    this.internalCode = internalCode;
    return this;
  }

  @ApiModelProperty(example = "2200", value = "The internal number referencing the error state")
  @JsonProperty("internalCode")
  public String getInternalCode() {
    return internalCode;
  }

  public void setInternalCode(String internalCode) {
    this.internalCode = internalCode;
  }

  /** The internal tracking number of the request */
  public AuthApiError internalTxId(String internalTxId) {
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
    if (!(o instanceof AuthApiError)) {
      return false;
    }
    AuthApiError error = (AuthApiError) o;
    return Objects.equals(description, error.description)
        && Objects.equals(internalCode, error.internalCode)
        && Objects.equals(internalTxId, error.internalTxId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, internalCode, internalTxId);
  }

  @Override
  public String toString() {
    return "Error{"
        + "description='"
        + description
        + '\''
        + ", internalCode='"
        + internalCode
        + '\''
        + ", internalTxId='"
        + internalTxId
        + '\''
        + '}';
  }
}
