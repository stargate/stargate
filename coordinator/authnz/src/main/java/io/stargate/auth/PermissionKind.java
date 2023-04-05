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

/**
 * Indicates a permission is given to access a resource (e.g. query data) or to permit
 * grating/revoking access to the resource to other users/roles.
 */
public enum PermissionKind {

  /**
   * Indicates the related permission controls access to the resource.
   *
   * <p>This flag generally corresponds to {@code GRANT/REVOKE <permission>} CQL statements.
   */
  ACCESS,

  /**
   * Indicates the related permission controls the authority to grant/revoke access the resource to
   * other users.
   *
   * <p>This flag generally corresponds to {@code GRANT/REVOKE AUTHORIZE FOR} CQL statements.
   */
  AUTHORITY,
}
