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
package io.stargate.db;

import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface AuthenticatedUser {

  String name();

  @Nullable
  String token();

  boolean isFromExternalAuth();

  static AuthenticatedUser of(String userName) {
    return ImmutableAuthenticatedUser.builder().name(userName).isFromExternalAuth(false).build();
  }

  static AuthenticatedUser of(String userName, String token) {
    return ImmutableAuthenticatedUser.builder()
        .name(userName)
        .token(token)
        .isFromExternalAuth(false)
        .build();
  }

  static AuthenticatedUser of(String userName, String token, boolean useTransitionalAuth) {
    return ImmutableAuthenticatedUser.builder()
        .name(userName)
        .token(token)
        .isFromExternalAuth(useTransitionalAuth)
        .build();
  }
}
