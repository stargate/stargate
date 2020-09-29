package org.apache.cassandra.stargate.db;

import java.util.List;
import org.apache.cassandra.stargate.schema.TableMetadata;

/** Represents a change made in the database. */
public interface MutationEvent {
  /** Gets the table metadata at the time of the event. */
  TableMetadata getTable();

  /** Timestamp of the change. */
  long getTimestamp();
  /** Gets the partitions keys of this event */
  List<CellValue> getPartitionKeys();
  /** Gets the clustering keys of this event */
  List<CellValue> getClusteringKeys();
}
