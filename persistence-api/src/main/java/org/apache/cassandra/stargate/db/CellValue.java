package org.apache.cassandra.stargate.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.stargate.schema.ColumnMetadata;

/** Represents a value. */
public interface CellValue {
  /** Gets the raw value of a cell. */
  ByteBuffer getValue();

  /** Uses a codec to provide the Java representation of the stored value. */
  Object getValueObject();

  /** Gets the column information associated with this cell value. */
  ColumnMetadata getColumn();
}
