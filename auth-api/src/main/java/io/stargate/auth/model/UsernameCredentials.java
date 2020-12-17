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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class UsernameCredentials {
  String username;

  @ApiModelProperty(
      required = true,
      value = "The username of the user within the database to generate a token for.")
  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  @JsonCreator
  public UsernameCredentials(@JsonProperty("username") String username) {
    this.username = username;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UsernameCredentials)) {
      return false;
    }

    UsernameCredentials that = (UsernameCredentials) o;

    return Objects.equals(username, that.username);
  }

  @Override
  public int hashCode() {
    return username != null ? username.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "UsernameCredentials{" + "username='" + username + '\'' + '}';
  }
}
