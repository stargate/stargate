package io.stargate.web.docsapi.service.json;

import java.util.Map;
import java.util.Set;

public interface DeadLeafCollector {

  void addLeaf(String path, DeadLeaf leaf);

  void addArray(String path);

  void addAll(String path);

  Map<String, Set<DeadLeaf>> getLeaves();

  boolean isEmpty();
}
