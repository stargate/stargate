package io.stargate.bridge.service.docsapi;

import io.stargate.db.RowDecorator;
import io.stargate.db.datastore.ResultSet;

/** A thin wrapper around {@link ResultSet} that caches {@link #decorator()} objects. */
@org.immutables.value.Value.Immutable(lazyhash = true)
public abstract class Page {

  abstract ResultSet resultSet();

  /** Lazily initialized and cached {@link RowDecorator}. */
  @org.immutables.value.Value.Lazy
  RowDecorator decorator() {
    return resultSet().makeRowDecorator();
  }
}
