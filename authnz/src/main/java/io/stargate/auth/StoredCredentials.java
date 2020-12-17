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
package io.stargate.auth;

import java.util.Objects;

/** StoredCredentials are a roleName and password mapping to one or more key-secret pairs. */
public final class StoredCredentials {
  private String roleName = null;
  private String password = null;

  public StoredCredentials roleName(String roleName) {
    this.roleName = roleName;
    return this;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public StoredCredentials password(String password) {
    this.password = password;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoredCredentials)) {
      return false;
    }
    StoredCredentials storedCredentials = (StoredCredentials) o;
    return Objects.equals(roleName, storedCredentials.roleName)
        && Objects.equals(password, storedCredentials.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(roleName, password);
  }

  @Override
  public String toString() {
    return "StoredCredentials{"
        + "roleName='"
        + roleName
        + '\''
        + ", password='"
        + password
        + '\''
        + '}';
  }
}
