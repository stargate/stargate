package org.apache.cassandra.stargate.db;

/** Represents a row cell containing a value, for regular columns. */
public interface Cell extends CellValue {
  /**
   * The cell ttl.
   *
   * @return the cell ttl, or {@code 0} if the cell isn't an expiring one.
   */
  int getTTL();

  /** Determines whether it's a tombstone */
  boolean isNull();
}
