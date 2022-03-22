package io.stargate.db.schema;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class EdgeLabelMetadata extends GraphLabel {
  public abstract VertexMappingMetadata fromVertex();

  public abstract VertexMappingMetadata toVertex();

  public static EdgeLabelMetadata create(
      String name, VertexMappingMetadata fromVertex, VertexMappingMetadata toVertex) {
    return ImmutableEdgeLabelMetadata.builder()
        .name(name)
        .fromVertex(fromVertex)
        .toVertex(toVertex)
        .build();
  }
}
