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
package io.stargate.it.storage;

import io.stargate.it.exec.OutputListener;
import java.io.File;
import java.util.List;

public interface StargateEnvironmentInfo {

  String id();

  File starterJarFile();

  List<? extends StargateConnectionInfo> nodes();

  /**
   * Add a node to the Stargate environment.
   *
   * @return the connection info associated with the added node. This can be used to remove this
   *     node from the environment using {@link #removeNode(StargateConnectionInfo)}.
   * @throws Exception
   * @throws UnsupportedOperationException when attempting to modify a shared environment.
   */
  StargateConnectionInfo addNode() throws Exception;

  /**
   * Remove a node from the Stargate environment. This is usually a node added via {@link
   * #addNode()}
   *
   * @param node the connection info for an existing Stargate node. This is usually returned from
   *     {@link #addNode()}, but could be a node returned by {@link #nodes()}.
   * @throws Exception
   * @throws UnsupportedOperationException when attempting to modify a shared environment.
   */
  void removeNode(StargateConnectionInfo node);

  void addStdOutListener(OutputListener listener);

  void removeStdOutListener(OutputListener listener);
}
