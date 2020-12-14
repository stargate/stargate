package org.apache.cassandra.stargate.db;

import io.stargate.db.schema.Table;
import java.util.List;

/** Represents a change made in the database. */
public interface MutationEvent {
  /** Gets the table metadata at the time of the event. */
  Table getTable();

  /** Timestamp of the change. */
  long getTimestamp();

  /** Gets the partitions keys of this event */
  List<CellValue> getPartitionKeys();

  /** Gets the clustering keys of this event */
  List<CellValue> getClusteringKeys();
}
