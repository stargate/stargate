package org.apache.cassandra.stargate.db;

import org.apache.cassandra.stargate.schema.TableMetadata;

/** Represents a change made in the database. */
public interface MutationEvent {
  /** Gets the table metadata at the time of the event. */
  TableMetadata getTable();

  /** Timestamp of the change. */
  long getTimestamp();
}
