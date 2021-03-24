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

/**
 * Identifies an entity (e.g. a keyspace) or a group of entities (e.g. all tables in a keyspace).
 *
 * <p>A "wildcard" selector, means "all resources" within their scope.
 */
public class EntitySelector {

  private final String name;

  private EntitySelector(String name) {
    this.name = name;
  }

  public static EntitySelector byName(String name) {
    return new EntitySelector(name);
  }

  public static EntitySelector wildcard() {
    return new EntitySelector(null);
  }

  public String getEntityName() {
    return name;
  }

  public boolean isWildcard() {
    return name == null;
  }

  @Override
  public String toString() {
    return name == null ? "*" : name;
  }
}
