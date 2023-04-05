package io.stargate.db.query;

import javax.annotation.Nullable;
import org.immutables.value.Value.Immutable;

@Immutable
public interface Modification {
  enum Operation {
    SET,
    INCREMENT,
    APPEND,
    PREPEND,
    REMOVE
  }

  ModifiableEntity entity();

  Operation operation();

  @Nullable
  TypedValue value();
}
