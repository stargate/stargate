package io.stargate.db;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class SimpleStatement extends Statement {
  private final String query;

  public SimpleStatement(String query, List<ByteBuffer> values, @Nullable List<String> boundNames) {
    super(values, boundNames);
    this.query = query;
  }

  public SimpleStatement(String query, List<ByteBuffer> values) {
    this(query, values, null);
  }

  public SimpleStatement(String query) {
    this(query, Collections.emptyList());
  }

  public String queryString() {
    return query;
  }

  @Override
  public String toString() {
    return String.format("%s (with %d values)", query, values().size());
  }
}
