package org.apache.cassandra.stargate.schema;

/**
 * Column information.
 *
 * <p>Backend agnostic representation equivalent to {@code
 * org.apache.cassandra.schema.ColumnMetadata}
 */
public interface ColumnMetadata {
  enum ClusteringOrder {
    ASC,
    DESC,
    NONE
  }

  enum Kind {
    PARTITION_KEY,
    CLUSTERING,
    REGULAR,
    STATIC
  }

  Kind getKind();

  /** Case-sensitive column name. */
  String getName();

  CQLType getType();
}
