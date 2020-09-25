package io.stargate.db.datastore.common.util;

import java.util.Objects;

// Basically an ExecutionException, but unchecked
public class UncheckedExecutionException extends RuntimeException {

  public UncheckedExecutionException(Throwable cause) {
    super(cause);
    Objects.requireNonNull(cause);
  }
}
