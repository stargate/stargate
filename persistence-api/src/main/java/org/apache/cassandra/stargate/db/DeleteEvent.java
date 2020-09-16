package org.apache.cassandra.stargate.db;

import java.util.List;

/** Represents a delete of one or multiple rows. */
public interface DeleteEvent extends MutationEvent {
  List<CellValue> getPartitionKeys();

  List<CellValue> getClusteringKeys();
}
