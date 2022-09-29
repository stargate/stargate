package io.stargate.web.docsapi.service.json;

import org.immutables.value.Value;

@Value.Immutable(singleton = true)
public interface DeadLeafCollector {
  default void addLeaf(String path, DeadLeaf leaf) {}

  default void addArray(String path) {}

  default void addAll(String path) {}

  default boolean isEmpty() {
    return true;
  }
}
