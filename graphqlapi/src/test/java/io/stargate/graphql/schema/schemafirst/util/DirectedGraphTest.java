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
package io.stargate.graphql.schema.schemafirst.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DirectedGraphTest {

  @Test
  @DisplayName("Should sort empty graph")
  public void sortEmpty() {
    DirectedGraph<String> g = new DirectedGraph<>();
    assertThat(g.topologicalSort()).isEmpty();
  }

  @Test
  @DisplayName("Should sort graph with one node")
  public void sortOneNode() {
    DirectedGraph<String> g = new DirectedGraph<>("A");
    assertThat(g.topologicalSort()).containsExactly("A");
  }

  @Test
  @DisplayName("Should sort complex graph")
  public void sortComplex() {
    //         H   G
    //        / \ /\
    //       F   |  E
    //        \ /  /
    //         D  /
    //        / \/
    //        B  C
    //        |
    //        A
    DirectedGraph<String> g = new DirectedGraph<>("A", "B", "C", "D", "E", "F", "G", "H");
    g.addEdge("H", "F");
    g.addEdge("G", "E");
    g.addEdge("H", "D");
    g.addEdge("F", "D");
    g.addEdge("G", "D");
    g.addEdge("D", "C");
    g.addEdge("E", "C");
    g.addEdge("D", "B");
    g.addEdge("B", "A");

    // The graph uses linked hash maps internally, so this order will be consistent across JVMs
    assertThat(g.topologicalSort()).containsExactly("G", "H", "E", "F", "D", "C", "B", "A");
  }

  @Test
  @DisplayName("Should fail to sort if graph has a cycle")
  public void sortWithCycle() {
    DirectedGraph<String> g = new DirectedGraph<>("A", "B", "C");
    g.addEdge("A", "B");
    g.addEdge("B", "C");
    g.addEdge("C", "B");

    assertThatThrownBy(g::topologicalSort).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should fail to sort if graph is cyclic")
  public void sortCyclic() {
    DirectedGraph<String> g = new DirectedGraph<>("A", "B", "C");
    g.addEdge("A", "B");
    g.addEdge("B", "C");
    g.addEdge("C", "A");

    assertThatThrownBy(g::topologicalSort).isInstanceOf(IllegalArgumentException.class);
  }
}
