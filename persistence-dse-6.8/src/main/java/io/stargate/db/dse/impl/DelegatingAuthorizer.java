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
package io.stargate.db.dse.impl;

import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;

import com.datastax.bdp.cassandra.auth.AuthRequestExecutionException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.stargate.exceptions.RequestValidationException;
import org.javatuples.Pair;

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
      RoleResource grantee,
      GrantMode... grantModes) {
    if (authProcessor == null) {
      return super.grant(performer, permissions, resource, grantee, grantModes);
    }

    Collection<CompletableFuture<Void>> stages = new ArrayList<>(grantModes.length);

    permissions = filterSupported(permissions);
    for (GrantMode grantMode : grantModes) {
      Pair<PermissionKind, AuthorizationOutcome> mode = grantMode(grantMode);
      CompletionStage<Void> stage =
          authProcessor.addPermissions(
              ImmutableActor.of(performer.getName()),
              mode.getValue1(),
              mode.getValue0(),
              permissions(permissions),
              resource(resource),
              role(grantee));

      stages.add(stage.toCompletableFuture());
    }

    get(CompletableFuture.allOf(stages.toArray(new CompletableFuture[0])));

    // assume all permissions were granted if we did not get an exception
    return permissions;
  }

  @Override
  public Set<Permission> revoke(
      AuthenticatedUser performer,
      Set<Permission> permissions,
      IResource resource,
      RoleResource revokee,
      GrantMode... grantModes) {
    if (authProcessor == null) {
      return super.revoke(performer, permissions, resource, revokee, grantModes);
    }

    Collection<CompletableFuture<Void>> stages = new ArrayList<>(grantModes.length);

    permissions = filterSupported(permissions);
    for (GrantMode grantMode : grantModes) {
      Pair<PermissionKind, AuthorizationOutcome> mode = grantMode(grantMode);
      CompletionStage<Void> stage =
          authProcessor.removePermissions(
              ImmutableActor.of(performer.getName()),
              mode.getValue1(),
              mode.getValue0(),
              permissions(permissions),
              resource(resource),
              role(revokee));

      stages.add(stage.toCompletableFuture());
    }

    get(CompletableFuture.allOf(stages.toArray(new CompletableFuture[0])));

    // assume all permissions were revoked if we did not get an exception
    return permissions;
  }

  private static void get(CompletionStage<Void> stage) {
    try {
      // wait for completion since the calling API is synchronous
      stage.toCompletableFuture().get(PROCESSING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RequestValidationException) {
        throw (RequestValidationException) cause;
      }

      throw new AuthRequestExecutionException(SERVER_ERROR, e.getMessage(), e);
    } catch (Exception e) {
      throw new AuthRequestExecutionException(SERVER_ERROR, e.getMessage(), e);
    }
  }

  private static Pair<PermissionKind, AuthorizationOutcome> grantMode(GrantMode grantMode) {
    switch (grantMode) {
      case GRANTABLE:
        return Pair.with(PermissionKind.AUTHORITY, AuthorizationOutcome.ALLOW);

      case GRANT:
        return Pair.with(PermissionKind.ACCESS, AuthorizationOutcome.ALLOW);

      case RESTRICT:
        return Pair.with(PermissionKind.ACCESS, AuthorizationOutcome.DENY);

      default:
        throw new UnsupportedOperationException("Unsupported grant mode: " + grantMode);
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
      } else if (dr.isAllTablesLevel()) {
        EntitySelector keyspace = EntitySelector.byName(dr.getKeyspace());
        authResource = ImmutableAuthorizedResource.of(ResourceKind.TABLE).withKeyspace(keyspace);
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

  private Set<Permission> filterSupported(Set<Permission> permissions) {
    return permissions.stream().filter(DelegatingAuthorizer::supported).collect(Collectors.toSet());
  }

  private static boolean supported(Permission permission) {
    return CorePermission.getDomain().equals(permission.domain());
  }

  private static Collection<AccessPermission> permissions(Set<Permission> permissions) {
    Collection<AccessPermission> accessPermissions = new ArrayList<>(permissions.size());
    for (Permission permission : permissions) {
      if (!supported(permission)) {
        throw new IllegalArgumentException("Unsupported permission: " + permission);
      }

      accessPermissions.add(ImmutableAccessPermission.of(permission.name()));
    }

    return accessPermissions;
  }
}
