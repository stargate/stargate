package org.apache.cassandra.stargate.db;

import java.util.List;

public interface RowMutationEvent extends MutationEvent {
  List<CellValue> getPartitionKeys();

  List<CellValue> getClusteringKeys();

  List<Cell> getCells();
}
