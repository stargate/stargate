package io.stargate.web.docsapi.service.json;

public enum DeadLeafCollectorNoOp implements DeadLeafCollector {
  INSTANCE;

  @Override
  public void addLeaf(String path, DeadLeaf leaf) {}

  @Override
  public void addArray(String path) {}

  @Override
  public void addAll(String path) {}

  @Override
  public boolean isEmpty() {
    return true;
  }
}
