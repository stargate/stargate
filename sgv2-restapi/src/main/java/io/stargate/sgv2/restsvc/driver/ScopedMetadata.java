/*
 * Copyright DataStax, Inc.
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
package io.stargate.sgv2.restsvc.driver;

import com.datastax.dse.driver.api.core.metadata.schema.DseGraphKeyspaceMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseKeyspaceMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

class ScopedMetadata implements Metadata {

  private final Metadata delegate;
  private final String keyspacePrefix;

  ScopedMetadata(Metadata delegate, String keyspacePrefix) {
    this.delegate = delegate;
    this.keyspacePrefix = keyspacePrefix;
  }

  @Override
  @NonNull
  public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
    // No need to cache this, it's unlikely that it will be called more than once per HTTP request
    return delegate.getKeyspaces().entrySet().stream()
        .filter(e -> e.getKey().asInternal().startsWith(keyspacePrefix))
        .collect(Collectors.toMap(Entry::getKey, e -> stripPrefix(e.getValue())));
  }

  private KeyspaceMetadata stripPrefix(KeyspaceMetadata keyspace) {
    String name = keyspace.getName().asInternal();
    assert name.startsWith(keyspacePrefix);
    CqlIdentifier newName = CqlIdentifier.fromInternal(name.substring(0, name.indexOf('_')));
    if (keyspace instanceof DefaultKeyspaceMetadata) {
      DefaultKeyspaceMetadata from = (DefaultKeyspaceMetadata) keyspace;
      return new DefaultKeyspaceMetadata(
          newName,
          from.isDurableWrites(),
          from.isVirtual(),
          from.getReplication(),
          from.getUserDefinedTypes(),
          from.getTables(),
          from.getViews(),
          from.getFunctions(),
          from.getAggregates());
    } else if (keyspace instanceof DseGraphKeyspaceMetadata) {
      DseGraphKeyspaceMetadata from = (DseGraphKeyspaceMetadata) keyspace;
      return new DefaultDseKeyspaceMetadata(
          newName,
          from.isDurableWrites(),
          from.isVirtual(),
          from.getGraphEngine().orElse(null),
          from.getReplication(),
          from.getUserDefinedTypes(),
          from.getTables(),
          from.getViews(),
          from.getFunctions(),
          from.getAggregates());
    } else {
      throw new AssertionError("Unexpected keyspace class " + keyspace.getClass().getName());
    }
  }

  @Override
  @NonNull
  public Map<UUID, Node> getNodes() {
    return delegate.getNodes();
  }

  @Override
  @NonNull
  public Optional<TokenMap> getTokenMap() {
    return delegate.getTokenMap();
  }

  @Override
  @NonNull
  public Optional<String> getClusterName() {
    return delegate.getClusterName();
  }
}
