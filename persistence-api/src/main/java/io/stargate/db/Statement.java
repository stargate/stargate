package io.stargate.db;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class Statement {
  private final List<ByteBuffer> values;
  private final @Nullable List<String> boundNames;

  protected Statement(List<ByteBuffer> values, @Nullable List<String> boundNames) {
    this.values = values;
    this.boundNames = boundNames;
  }

  public List<ByteBuffer> values() {
    return values;
  }

  public Optional<List<String>> boundNames() {
    return Optional.ofNullable(boundNames);
  }
}
