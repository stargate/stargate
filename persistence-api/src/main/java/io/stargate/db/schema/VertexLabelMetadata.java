package io.stargate.db.schema;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class VertexLabelMetadata extends GraphLabel {
  public static VertexLabelMetadata create(String name) {
    return ImmutableVertexLabelMetadata.builder().name(name).build();
  }
}
