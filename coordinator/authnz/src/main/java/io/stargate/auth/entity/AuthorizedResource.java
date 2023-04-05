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

import org.immutables.value.Value;

/** Identifies a resource for authorization purposes. */
@Value.Immutable
public interface AuthorizedResource {

  /** Identifies the resource kind. */
  @Value.Parameter
  ResourceKind kind();

  /**
   * Identifies the keyspace of the resource.
   *
   * <p>If the keyspace selector is a wildcard, the {@link #element()} selector should also be a
   * wildcard.
   */
  @Value.Default
  default EntitySelector keyspace() {
    return EntitySelector.wildcard();
  }

  /** Identifies the name of the resource. */
  @Value.Default
  default EntitySelector element() {
    return EntitySelector.wildcard();
  }
}
