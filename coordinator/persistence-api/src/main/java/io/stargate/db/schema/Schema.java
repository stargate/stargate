/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class Schema {
  private static final Keyspace ANONYMOUS = ImmutableKeyspace.builder().name("<anonymous>").build();

  public abstract Set<Keyspace> keyspaces();

  @Value.Lazy
  Map<String, Keyspace> keyspaceMap() {
    return keyspaces().stream().collect(Collectors.toMap(Keyspace::name, Function.identity()));
  }

  public Keyspace keyspace(String name) {
    if (name == null) {
      return ANONYMOUS;
    }

    return keyspaceMap().get(name);
  }

  public List<String> keyspaceNames() {
    return keyspaces().stream().map(k -> k.name()).sorted().collect(Collectors.toList());
  }

  public static Schema create(Iterable<Keyspace> keyspaces) {
    return ImmutableSchema.builder().addAllKeyspaces(keyspaces).build();
  }

  @Override
  public String toString() {
    return keyspaces().stream().map(Keyspace::toString).collect(Collectors.joining("\n"));
  }
}
