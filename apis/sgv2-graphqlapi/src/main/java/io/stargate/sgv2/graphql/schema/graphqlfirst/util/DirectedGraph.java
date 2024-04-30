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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.graphql.schema.graphqlfirst.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A basic directed graph implementation to perform topological sorts.
 *
 * <p>This class is <b>not</b> a general-purpose graph implementation: it is only intended for
 * clients that will build a graph, sort it, and then immediately throw it away. In particular, once
 * {@link #topologicalSort()} has been invoked, it is not possible to add vertices or sort again.
 */
public class DirectedGraph<VertexT> {

  /** How many predecessors each vertex has. */
  private final Map<VertexT, Integer> predecessorCounts;

  /** The list successors of each vertex. */
  private final Multimap<VertexT, VertexT> adjacencyList;

  private boolean wasSorted;

  public DirectedGraph(Collection<VertexT> vertices) {
    this.predecessorCounts = Maps.newLinkedHashMapWithExpectedSize(vertices.size());
    this.adjacencyList = LinkedHashMultimap.create();

    for (VertexT vertex : vertices) {
      this.predecessorCounts.put(vertex, 0);
    }
  }

  @VisibleForTesting
  @SafeVarargs
  DirectedGraph(VertexT... vertices) {
    this(Arrays.asList(vertices));
  }

  /**
   * @throws IllegalStateException if {@link #topologicalSort()} was invoked before
   * @throws IllegalArgumentException if {@code from} or {@code to} is not part of the vertices
   *     passed to the constructor
   */
  public void addEdge(VertexT from, VertexT to) {
    Preconditions.checkState(!wasSorted);
    Preconditions.checkArgument(
        predecessorCounts.containsKey(from) && predecessorCounts.containsKey(to));
    adjacencyList.put(from, to);
    predecessorCounts.put(to, predecessorCounts.get(to) + 1);
  }

  /**
   * Performs a topological sort of the graph: for every directed edge {@code from->to}, {@code
   * from} will come before {@code to} in the resulting list.
   *
   * <p>Implementation note: this is based on <a
   * href="https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm">Kahn's algorithm</a>,
   * the running time is linear in the number of nodes plus the number of edges.
   *
   * @throws IllegalStateException if this method was already invoked on this graph before
   * @throws IllegalArgumentException if the graph has a cycle
   */
  public List<VertexT> topologicalSort() {
    Preconditions.checkState(!wasSorted);
    wasSorted = true;

    Queue<VertexT> startNodes = new ArrayDeque<>();
    List<VertexT> result = Lists.newArrayList();

    // Start with all the nodes that have no predecessors
    for (Map.Entry<VertexT, Integer> entry : predecessorCounts.entrySet()) {
      if (entry.getValue() == 0) {
        startNodes.add(entry.getKey());
      }
    }

    // For each start node:
    // - move it to the result
    // - for each of its successors, decrement the predecessor count, and possibly promote to a
    //   start node
    while (!startNodes.isEmpty()) {
      VertexT vertex = startNodes.remove();
      result.add(vertex);
      for (VertexT successor : adjacencyList.get(vertex)) {
        if (decrementAndGetCount(successor) == 0) {
          startNodes.add(successor);
        }
      }
    }

    // If we've moved every node to the result, we have a solution, otherwise there is a cycle
    if (result.size() != predecessorCounts.size()) {
      throw new IllegalArgumentException("failed to perform topological sort, graph has a cycle");
    }

    return result;
  }

  @SuppressWarnings("")
  private int decrementAndGetCount(VertexT vertex) {
    return predecessorCounts.compute(
        vertex,
        (__, count) -> {
          assert count != null;
          return count - 1;
        });
  }
}
