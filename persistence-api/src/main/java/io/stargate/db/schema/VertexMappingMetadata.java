package io.stargate.db.schema;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class VertexMappingMetadata implements Serializable {
  public abstract ImmutableList<Column> columns();

  public abstract String vertexTableName();

  public abstract List<ColumnMappingMetadata> columnMappings();

  @Value.Lazy
  public ImmutableList<Column> partitionKeyColumns() {
    return ImmutableList.copyOf(
        columns().stream()
            .filter(c -> c.kind() == Column.Kind.PartitionKey)
            .collect(Collectors.toList()));
  }

  @Value.Lazy
  public ImmutableList<Column> clusteringKeyColumns() {
    return ImmutableList.copyOf(
        columns().stream()
            .filter(c -> c.kind() == Column.Kind.Clustering)
            .collect(Collectors.toList()));
  }

  @Value.Lazy
  public ImmutableSet<String> columnNames() {
    Set<String> set =
        columns().stream().map(c -> c.name()).collect(Collectors.toCollection(HashSet::new));
    return ImmutableSet.copyOf(set);
  }

  @Value.Lazy
  public Map<String, String> vertexToEdgeColumnMapping() {
    return columnMappings().stream()
        .collect(
            Collectors.toMap(
                ColumnMappingMetadata::vertexColumn, ColumnMappingMetadata::edgeColumn));
  }

  @Value.Lazy
  public Map<String, String> edgeToVertexColumnMapping() {
    return columnMappings().stream()
        .collect(
            Collectors.toMap(
                ColumnMappingMetadata::edgeColumn, ColumnMappingMetadata::vertexColumn));
  }

  public static VertexMappingMetadata create(String vertexTableName, List<Column> columns) {
    return ImmutableVertexMappingMetadata.builder()
        .vertexTableName(vertexTableName)
        .columns(ImmutableList.copyOf(columns))
        .build();
  }

  public static VertexMappingMetadata create(
      String vertexTableName, List<Column> columns, List<ColumnMappingMetadata> columnMappings) {
    return ImmutableVertexMappingMetadata.builder()
        .vertexTableName(vertexTableName)
        .columns(ImmutableList.copyOf(columns))
        .columnMappings(columnMappings)
        .build();
  }
}
