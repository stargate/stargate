package io.stargate.web.docsapi.service.json;

public interface DeadLeafCollector {
  void addLeaf(String path, DeadLeaf leaf);

  void addArray(String path);

  void addAll(String path);

  boolean isEmpty();
}
