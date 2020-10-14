package io.stargate.db;

import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BoundStatement extends Statement {
  private final MD5Digest id;

  public BoundStatement(MD5Digest id, List<ByteBuffer> values, @Nullable List<String> boundNames) {
    super(values, boundNames);
    this.id = id;
  }

  public MD5Digest preparedId() {
    return id;
  }

  @Override
  public String toString() {
    return String.format("Prepared %s (with %d values)", preparedId(), values().size());
  }
}
