package io.stargate.web.docsapi.service.json;

public interface DeadLeafCollector {
  default void addLeaf(String path, DeadLeaf leaf) {}

  default void addArray(String path) {}

  default void addAll(String path) {}

  default boolean isEmpty() {
    return true;
  }
}
