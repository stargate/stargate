package io.stargate.db;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class Statement {
  private final List<ByteBuffer> values;
  private final @Nullable List<String> boundNames;

  protected Statement(List<ByteBuffer> values, @Nullable List<String> boundNames) {
    Preconditions.checkArgument(
        boundNames == null || values.size() == boundNames.size(),
        "Expected the number of values %s to be equal to the number of bound names %s",
        values.size(),
        boundNames == null ? 0 : boundNames.size());
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
