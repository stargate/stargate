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

import io.stargate.auth.entity.AccessPermission;
import io.stargate.auth.entity.Actor;
import io.stargate.auth.entity.AuthorizedResource;
import io.stargate.auth.entity.EntitySelector;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * An abstraction of the authorization command processor in Stargate.
 *
 * <p>This interface is intended to broadly cover the functionality of CQL GRANT/REVOKE commands in
 * a programmatic API.
 */
public interface AuthorizationProcessor {

  /**
   * Grants {@code grantee} the specified access {@code permissions} on the specified {@code
   * resource}. Note that access can be positive ({@link AuthorizationOutcome#ALLOW} or negative
   * {@link AuthorizationOutcome#DENY}. Negative permissions have precedence over positive
   * permissions.
   *
   * @param performer the user under whose authority the grant operation is performed
   * @param outcome whether operations under the specified permissions are allowed or denied
   * @param kind whether permission is granted to access the resource or control access to it
   * @param permissions the list of permissions to be granted
   * @param resource the resource whose access permissions are granted
   * @param grantee the role receiving the permissions
   * @return the {@link CompletionStage} tracking the progress of the grant operation.
   */
  CompletionStage<Void> addPermissions(
      Actor performer,
      AuthorizationOutcome outcome,
      PermissionKind kind,
      Collection<AccessPermission> permissions,
      AuthorizedResource resource,
      EntitySelector grantee);

  /**
   * Revokes the specified access {@code permissions} on the specified {@code resource} from the
   * {@code grantee}. Note that revoking a {@link AuthorizationOutcome#DENY negative} permission is
   * essentially widening access to the resource.
   *
   * @param performer the user under whose authority the revocation operation is performed
   * @param outcome whether operations under the specified permissions are allowed or denied
   * @param kind whether permission is granted to access the resource or control access to it
   * @param permissions the list of permissions to be revoked
   * @param resource the resource whose access permissions are revoked
   * @param grantee the role whose permissions are revoked
   * @return the {@link CompletionStage} tracking the progress of the grant operation.
   */
  CompletionStage<Void> removePermissions(
      Actor performer,
      AuthorizationOutcome outcome,
      PermissionKind kind,
      Collection<AccessPermission> permissions,
      AuthorizedResource resource,
      EntitySelector grantee);
}
