package io.stargate.db.cdc.serde;

import io.stargate.db.query.BoundDMLQuery;
import java.nio.ByteBuffer;

public class QuerySerializer {
  public static ByteBuffer serializeQuery(BoundDMLQuery boundDMLQuery) {
    return ByteBuffer.allocate(1);
  }
}
