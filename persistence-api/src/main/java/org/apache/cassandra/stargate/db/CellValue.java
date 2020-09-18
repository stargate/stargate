package org.apache.cassandra.stargate.db;

import java.nio.ByteBuffer;

/** Represents a value. */
public interface CellValue {
  /** Gets the raw value of a cell. */
  ByteBuffer getValue();

  /** Uses a codec to provide the Java representation of the stored value. */
  Object getValueObject();
}
