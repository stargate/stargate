package io.stargate.web.docsapi.service.json;

import java.util.*;

public class DeadLeafCollectorImpl implements DeadLeafCollector {
  private Map<String, Set<DeadLeaf>> deadPaths;

  public DeadLeafCollectorImpl() {
    deadPaths = new HashMap<>();
  }

  public void addLeaf(String path, DeadLeaf leaf) {
    Set<DeadLeaf> leavesAtPath = deadPaths.getOrDefault(path, new HashSet<>());
    leavesAtPath.add(leaf);
    deadPaths.put(path, leavesAtPath);
  }

  public void addArray(String path) {
    Set<DeadLeaf> leavesAtPath = deadPaths.getOrDefault(path, new HashSet<>());
    leavesAtPath.add(ImmutableDeadLeaf.builder().name(DeadLeaf.ARRAY).build());
    deadPaths.put(path, leavesAtPath);
  }

  public void addAll(String path) {
    Set<DeadLeaf> leavesAtPath = new HashSet<>();
    leavesAtPath.add(ImmutableDeadLeaf.builder().name(DeadLeaf.STAR).build());
    deadPaths.put(path, leavesAtPath);
  }

  public Map<String, Set<DeadLeaf>> getLeaves() {
    return deadPaths;
  }

  public boolean isEmpty() {
    return deadPaths.isEmpty();
  }
}
