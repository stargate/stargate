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
package io.stargate.db.cassandra.impl;

import io.stargate.auth.AuthorizationOutcome;
import io.stargate.auth.AuthorizationProcessor;
import io.stargate.auth.PermissionKind;
import io.stargate.auth.entity.AccessPermission;
import io.stargate.auth.entity.AuthorizedResource;
import io.stargate.auth.entity.EntitySelector;
import io.stargate.auth.entity.ImmutableAccessPermission;
import io.stargate.auth.entity.ImmutableActor;
import io.stargate.auth.entity.ImmutableAuthorizedResource;
import io.stargate.auth.entity.ResourceKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;

public class DelegatingAuthorizer extends CassandraAuthorizer {

  private static final Duration PROCESSING_TIMEOUT =
      Duration.parse(System.getProperty("stargate.authorization.processing.timeout", "PT5M"));

  private AuthorizationProcessor authProcessor;

  public void setProcessor(AuthorizationProcessor processor) {
    authProcessor = processor;
  }

  @Override
  public Set<Permission> grant(
      AuthenticatedUser performer,
      Set<Permission> permissions,
      IResource resource,
      RoleResource grantee)
      throws RequestValidationException, RequestExecutionException {
    if (authProcessor == null) {
      return super.grant(performer, permissions, resource, grantee);
    }

    CompletionStage<Void> stage =
        authProcessor.addPermissions(
            ImmutableActor.of(performer.getName()),
            AuthorizationOutcome.ALLOW,
            PermissionKind.ACCESS,
            permissions(permissions),
            resource(resource),
            role(grantee));

    get(stage);
    return permissions;
  }

  @Override
  public Set<Permission> revoke(
      AuthenticatedUser performer,
      Set<Permission> permissions,
      IResource resource,
      RoleResource revokee)
      throws RequestValidationException, RequestExecutionException {
    if (authProcessor == null) {
      return super.revoke(performer, permissions, resource, revokee);
    }

    CompletionStage<Void> stage =
        authProcessor.removePermissions(
            ImmutableActor.of(performer.getName()),
            AuthorizationOutcome.ALLOW,
            PermissionKind.ACCESS,
            permissions(permissions),
            resource(resource),
            role(revokee));

    get(stage);
    return permissions;
  }

  private static void get(CompletionStage<Void> stage) {
    try {
      // wait for completion since the calling API is synchronous
      stage.toCompletableFuture().get(PROCESSING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof org.apache.cassandra.stargate.exceptions.RequestValidationException) {
        throw (org.apache.cassandra.stargate.exceptions.RequestValidationException) cause;
      }

      throw new AuthenticationException(e.getMessage(), e);
    } catch (Exception e) {
      throw new AuthenticationException(e.getMessage(), e);
    }
  }

  private static EntitySelector role(RoleResource roleResource) {
    if (!roleResource.hasParent()) { // root == all roles
      return EntitySelector.wildcard();
    } else {
      return EntitySelector.byName(roleResource.getRoleName());
    }
  }

  private static AuthorizedResource resource(IResource resource) {
    String resourcePath = resource.getName();
    ImmutableAuthorizedResource authResource;

    if (resource instanceof FunctionResource) {
      authResource = ImmutableAuthorizedResource.of(ResourceKind.FUNCTION);

      FunctionResource fr = (FunctionResource) resource;

      boolean hasKeyspace = resourcePath.indexOf("/") > 0;
      if (hasKeyspace) {
        EntitySelector keyspace = EntitySelector.byName(fr.getKeyspace());
        authResource = authResource.withKeyspace(keyspace);

        String subPath = resourcePath.substring(resourcePath.indexOf('/'));
        int idx = subPath.indexOf('/');
        if (idx > 0) {
          EntitySelector element = EntitySelector.byName(subPath.substring(idx));
          authResource = authResource.withElement(element);
        }
      }
    } else if (resource instanceof DataResource) {
      DataResource dr = (DataResource) resource;

      // Note: root-level data resources have default wildcard keyspace and element selectors
      if (dr.isRootLevel()) {
        authResource = ImmutableAuthorizedResource.of(ResourceKind.KEYSPACE);
      } else if (dr.isKeyspaceLevel()) {
        EntitySelector keyspace = EntitySelector.byName(dr.getKeyspace());
        authResource = ImmutableAuthorizedResource.of(ResourceKind.KEYSPACE).withKeyspace(keyspace);
      } else if (dr.isTableLevel()) {
        EntitySelector keyspace = EntitySelector.byName(dr.getKeyspace());
        EntitySelector element = EntitySelector.byName(dr.getTable());
        authResource =
            ImmutableAuthorizedResource.of(ResourceKind.TABLE)
                .withKeyspace(keyspace)
                .withElement(element);
      } else {
        throw new IllegalArgumentException("Unsupported data resource: " + dr);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported resource type: " + resource.getClass());
    }

    return authResource;
  }

  private static Collection<AccessPermission> permissions(Set<Permission> permissions) {
    Collection<AccessPermission> accessPermissions = new ArrayList<>(permissions.size());
    for (Permission permission : permissions) {
      accessPermissions.add(ImmutableAccessPermission.of(permission.name()));
    }

    return accessPermissions;
  }
}
