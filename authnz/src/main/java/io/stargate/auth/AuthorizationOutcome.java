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

/** Indicates whether access to a resource under the related permission is granted or denied. */
public enum AuthorizationOutcome {

  /**
   * Indicates that access to the related resource is allowed.
   *
   * <p>This flag generally corresponds to {@code GRANT} CQL statements.
   */
  ALLOW,

  /**
   * Indicates that access to the related resource is denied.
   *
   * <p>This flag generally corresponds to {@code RESTRICT} CQL statements.
   */
  DENY,
}
