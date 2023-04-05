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

import io.stargate.db.AuthenticatedUser;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface AuthenticationSubject {

  @Nullable
  String token();

  String roleName();

  boolean isFromExternalAuth();

  Map<String, String> customProperties();

  default AuthenticatedUser asUser() {
    return AuthenticatedUser.of(roleName(), token(), isFromExternalAuth(), customProperties());
  }

  static AuthenticationSubject of(
      String token, String roleName, boolean fromExternalAuth, Map<String, String> properties) {
    return ImmutableAuthenticationSubject.builder()
        .token(token)
        .roleName(roleName)
        .isFromExternalAuth(fromExternalAuth)
        .customProperties(properties)
        .build();
  }

  static AuthenticationSubject of(String token, String roleName, boolean fromExternalAuth) {
    return ImmutableAuthenticationSubject.builder()
        .token(token)
        .roleName(roleName)
        .isFromExternalAuth(fromExternalAuth)
        .customProperties(Collections.emptyMap())
        .build();
  }

  static AuthenticationSubject of(String token, String roleName) {
    return ImmutableAuthenticationSubject.builder()
        .token(token)
        .roleName(roleName)
        .isFromExternalAuth(false)
        .customProperties(Collections.emptyMap())
        .build();
  }

  static AuthenticationSubject of(AuthenticatedUser user) {
    return ImmutableAuthenticationSubject.builder()
        .token(user.token())
        .roleName(user.name())
        .isFromExternalAuth(user.isFromExternalAuth())
        .customProperties(user.customProperties())
        .build();
  }
}
