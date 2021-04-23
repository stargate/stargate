package io.stargate.web.docsapi.service.json;

import java.util.Map;
import java.util.Set;

public class DeadLeafCollectorNoOp implements DeadLeafCollector {
  public void addLeaf(String path, DeadLeaf leaf) {}

  public void addArray(String path) {}

  public void addAll(String path) {}

  public Map<String, Set<DeadLeaf>> getLeaves() {
    throw new UnsupportedOperationException("No-op implementation doesn't have leaves");
  }

  public boolean isEmpty() {
    return true;
  }
}
