package org.apache.cassandra.stargate.db;

import java.nio.ByteBuffer;

/** Represents a value. */
public interface CellValue {
  ByteBuffer getValue();
}
