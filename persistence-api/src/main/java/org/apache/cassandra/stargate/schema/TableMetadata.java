package org.apache.cassandra.stargate.schema;

import java.util.List;
import java.util.UUID;

/**
 * Table information.
 *
 * <p>Backend agnostic representation equivalent to {@code
 * org.apache.cassandra.schema.TableMetadata}
 */
public interface TableMetadata {
  /** Gets the table id. */
  UUID getId();

  String getKeyspace();

  String getName();

  List<ColumnMetadata> getPartitionKeys();

  List<ColumnMetadata> getClusteringKeys();

  /** A collection of static and regular columns. */
  List<ColumnMetadata> getColumns();
}
