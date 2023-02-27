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
package io.stargate.auth.entity;

import io.stargate.auth.PermissionKind;
import io.stargate.db.Persistence;
import org.immutables.value.Value;

/**
 * Represents a permission from a particular {@link Persistence} implementation.
 *
 * <p>From Stargate perspective only the name of the permission matters.
 */
@Value.Immutable
public interface AccessPermission {

  /** The name of the permission relative to its {@link PermissionKind}. */
  @Value.Parameter
  String name();
}
