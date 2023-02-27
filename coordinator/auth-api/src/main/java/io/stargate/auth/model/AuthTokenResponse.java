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
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;

/** AuthTokenResponse contains an authentication token to be used for future requests. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class AuthTokenResponse {
  private String authToken = null;

  public AuthTokenResponse authToken(String authToken) {
    this.authToken = authToken;
    return this;
  }

  @JsonProperty("authToken")
  @ApiModelProperty(
      required = true,
      value = "The authentication token to use for authorizing other requests.")
  public String getAuthToken() {
    return authToken;
  }

  public void setAuthToken(String authToken) {
    this.authToken = authToken;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuthTokenResponse)) {
      return false;
    }
    AuthTokenResponse authTokenResponse = (AuthTokenResponse) o;
    return Objects.equals(authToken, authTokenResponse.authToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authToken);
  }

  @Override
  public String toString() {
    return "AuthTokenResponse{" + "authToken='" + authToken + '\'' + '}';
  }
}
