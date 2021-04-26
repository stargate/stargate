package io.stargate.web.docsapi.service.json;

public class DeadLeafCollectorNoOp implements DeadLeafCollector {
  public void addLeaf(String path, DeadLeaf leaf) {}

  public void addArray(String path) {}

  public void addAll(String path) {}

  public boolean isEmpty() {
    return true;
  }
}
