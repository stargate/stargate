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

/** Secret contains the key and secret for authentication */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Secret {
  private String key = null;
  private String secret = null;

  @ApiModelProperty(required = true, value = "The key to authenticate with.")
  @JsonProperty("key")
  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @ApiModelProperty(required = true, value = "Secret to authenticate with.")
  @JsonProperty("secret")
  public String getSecret() {
    return secret;
  }

  public void setSecret(String secret) {
    this.secret = secret;
  }

  @JsonCreator
  public Secret(@JsonProperty("key") String key, @JsonProperty("secret") String secret) {
    this.key = key;
    this.secret = secret;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Secret)) {
      return false;
    }
    Secret secret = (Secret) o;
    return Objects.equals(this.key, secret.key) && Objects.equals(this.secret, secret.secret);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, secret);
  }

  @Override
  public String toString() {
    return "Secret{" + "key='" + key + '\'' + ", secret='" + secret + '\'' + '}';
  }
}
