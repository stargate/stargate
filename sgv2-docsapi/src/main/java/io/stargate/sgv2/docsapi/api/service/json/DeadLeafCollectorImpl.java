package io.stargate.sgv2.docsapi.api.service.json;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeadLeafCollectorImpl implements DeadLeafCollector {
  private final Map<String, Set<DeadLeaf>> deadPaths;

  public DeadLeafCollectorImpl() {
    deadPaths = new HashMap<>();
  }

  @Override
  public void addLeaf(String path, DeadLeaf leaf) {
    Set<DeadLeaf> leavesAtPath = deadPaths.getOrDefault(path, new HashSet<>());
    leavesAtPath.add(leaf);
    deadPaths.put(path, leavesAtPath);
  }

  @Override
  public void addArray(String path) {
    Set<DeadLeaf> leavesAtPath = deadPaths.getOrDefault(path, new HashSet<>());
    leavesAtPath.add(DeadLeaf.ARRAYLEAF);
    deadPaths.put(path, leavesAtPath);
  }

  @Override
  public void addAll(String path) {
    Set<DeadLeaf> leavesAtPath = new HashSet<>();
    leavesAtPath.add(DeadLeaf.STARLEAF);
    deadPaths.put(path, leavesAtPath);
  }

  public Map<String, Set<DeadLeaf>> getLeaves() {
    return deadPaths;
  }

  @Override
  public boolean isEmpty() {
    return deadPaths.isEmpty();
  }
}
