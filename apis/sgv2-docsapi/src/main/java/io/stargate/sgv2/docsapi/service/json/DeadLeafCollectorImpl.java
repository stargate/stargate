/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.json;

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
    leavesAtPath.add(DeadLeaf.ARRAY_LEAF);
    deadPaths.put(path, leavesAtPath);
  }

  @Override
  public void addAll(String path) {
    Set<DeadLeaf> leavesAtPath = new HashSet<>();
    leavesAtPath.add(DeadLeaf.STAR_LEAF);
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
