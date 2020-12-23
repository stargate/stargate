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

public class AuthenticationSubject {

  private String token;
  private String roleName;
  private boolean fromExternalAuth;

  public AuthenticationSubject(String token, String roleName, boolean fromExternalAuth) {
    this.token = token;
    this.roleName = roleName;
    this.fromExternalAuth = fromExternalAuth;
  }

  public AuthenticationSubject(String token, String roleName) {
    this.token = token;
    this.roleName = roleName;
    this.fromExternalAuth = false;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public boolean isFromExternalAuth() {
    return fromExternalAuth;
  }

  public void setFromExternalAuth(boolean fromExternalAuth) {
    this.fromExternalAuth = fromExternalAuth;
  }
}
