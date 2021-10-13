/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.dse.impl;

import com.google.common.base.Suppliers;
import java.util.Collections;
import java.util.function.Supplier;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionSets;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;

/**
 * This is a special role used for externally authenticated users. When used, we trust the user
 * query has been validated externally and just let it through our role checks. It is not however a
 * system or super user role so that guardrail checks still happen
 */
public class ExternallyAuthenticatedUserRole extends UserRolesAndPermissions {

  public static final Supplier<ExternallyAuthenticatedUserRole> INSTANCE =
      Suppliers.memoize(ExternallyAuthenticatedUserRole::new)::get;

  private ExternallyAuthenticatedUserRole() {
    super("ExternalUserRole", Collections.emptySet());
  }

  @Override
  public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
    return false;
  }

  @Override
  public boolean hasGrantPermission(IResource resource, Permission perm) {
    return false;
  }

  @Override
  protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
    // intentionally empty
  }

  @Override
  public boolean hasPermissionOnResource(IResource resource, Permission perm) {
    return true;
  }

  @Override
  public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
    // intentionally empty
  }

  @Override
  public UserRolesAndPermissions cloneWithoutAdditionalPermissions() {
    // Immutable so ok
    return this;
  }
}
