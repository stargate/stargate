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
package io.stargate.testing.auth;

import io.stargate.auth.AuthorizationOutcome;
import io.stargate.auth.AuthorizationProcessor;
import io.stargate.auth.PermissionKind;
import io.stargate.auth.entity.AccessPermission;
import io.stargate.auth.entity.Actor;
import io.stargate.auth.entity.AuthorizedResource;
import io.stargate.auth.entity.EntitySelector;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingAuthorizationProcessorImpl implements AuthorizationProcessor {

  private static final Logger log =
      LoggerFactory.getLogger(LoggingAuthorizationProcessorImpl.class);

  @Override
  public CompletionStage<Void> addPermissions(
      Actor performer,
      AuthorizationOutcome outcome,
      PermissionKind kind,
      Collection<AccessPermission> permissions,
      AuthorizedResource resource,
      EntitySelector grantee) {
    log.warn(
        "testing: addPermissions: {}, {}, {}, {}, {}, {}",
        performer.roleName(),
        outcome,
        kind,
        permissions.stream()
            .map(AccessPermission::name)
            .collect(Collectors.toCollection(TreeSet::new)),
        resource,
        grantee);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<Void> removePermissions(
      Actor performer,
      AuthorizationOutcome outcome,
      PermissionKind kind,
      Collection<AccessPermission> permissions,
      AuthorizedResource resource,
      EntitySelector grantee) {
    log.warn(
        "testing: removePermissions: {}, {}, {}, {}, {}, {}",
        performer.roleName(),
        outcome,
        kind,
        permissions.stream()
            .map(AccessPermission::name)
            .collect(Collectors.toCollection(TreeSet::new)),
        resource,
        grantee);
    return CompletableFuture.completedFuture(null);
  }
}
