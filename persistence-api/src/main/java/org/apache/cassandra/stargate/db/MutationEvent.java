package org.apache.cassandra.stargate.db;

import org.apache.cassandra.stargate.schema.TableMetadata;

/**
 * Represents a change made in the database.
 *
 * <p>Backend agnostic representation of equivalent to {@code org.apache.cassandra.db.Mutation}
 */
public interface MutationEvent {
  /** Gets the table metadata at the time of the event. */
  TableMetadata getTable();

  /** Timestamp of the change. */
  long getTimestamp();
}
