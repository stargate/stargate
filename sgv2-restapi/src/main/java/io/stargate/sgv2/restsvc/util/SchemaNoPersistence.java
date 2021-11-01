package io.stargate.sgv2.restsvc.util;

import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bogus {@link Schema} implementation that we need for Stargate V2 as we do not have actual
 * Persistence implementation. This means that keyspace and table entities are constructed on-demand
 * without validation for existence.
 */
public class SchemaNoPersistence extends Schema {
  private static final Keyspace ANONYMOUS = ImmutableKeyspace.builder().name("<anonymous>").build();

  @Override
  public Set<Keyspace> keyspaces() {
    return Collections.emptySet();
  }

  @Override
  public Keyspace keyspace(String name) {
    if (name == null) {
      return ANONYMOUS;
    }
    return ImmutableKeyspace.builder().name(name).build();
  }

  public List<String> keyspaceNames() {
    return keyspaces().stream().map(k -> k.name()).sorted().collect(Collectors.toList());
  }
}
