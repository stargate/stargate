package org.apache.cassandra.stargate.schema;

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
}
