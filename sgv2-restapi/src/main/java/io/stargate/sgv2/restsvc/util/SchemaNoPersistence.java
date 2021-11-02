package io.stargate.sgv2.restsvc.util;

import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Bogus {@link Schema} implementation that we need for Stargate V2 as we do not have actual
 * Persistence implementation. This means that keyspace and table entities are constructed on-demand
 * without validation for existence.
 *
 * <p>NOTE: very much prototype level, unlikely to be used in this exact form for eventual Stargate
 * V2.
 */
public class SchemaNoPersistence extends Schema {
  @Override
  public Set<Keyspace> keyspaces() {
    return Collections.emptySet();
  }

  @Override
  public Keyspace keyspace(String name) {
    if ((name == null) || name.isEmpty()) {
      throw new IllegalArgumentException(
          "Can not create a keyspace without non-null, non-empty \"name\"");
    }
    return ImmutableKeyspace.builder().name(name).build();
  }

  public List<String> keyspaceNames() {
    return Collections.emptyList();
  }
}
