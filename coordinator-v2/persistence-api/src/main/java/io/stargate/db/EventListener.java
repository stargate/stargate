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
package io.stargate.db;

import java.net.InetAddress;
import java.util.List;

public interface EventListener {
  int NO_PORT = -1;

  default void onCreateKeyspace(String keyspace) {}

  default void onCreateTable(String keyspace, String table) {}

  default void onCreateView(String keyspace, String view) {
    onCreateTable(keyspace, view);
  }

  default void onCreateType(String keyspace, String type) {}

  default void onCreateFunction(String keyspace, String function, List<String> argumentTypes) {}

  default void onCreateAggregate(String keyspace, String aggregate, List<String> argumentTypes) {}

  default void onAlterKeyspace(String keyspace) {}

  default void onAlterTable(String keyspace, String table) {}

  default void onAlterView(String keyspace, String view) {
    onAlterTable(keyspace, view);
  }

  default void onAlterType(String keyspace, String type) {}

  default void onAlterFunction(String keyspace, String function, List<String> argumentTypes) {}

  default void onAlterAggregate(String keyspace, String aggregate, List<String> argumentTypes) {}

  default void onDropKeyspace(String keyspace) {}

  default void onDropTable(String keyspace, String table) {}

  default void onDropView(String keyspace, String view) {
    onDropTable(keyspace, view);
  }

  default void onDropType(String keyspace, String type) {}

  default void onDropFunction(String keyspace, String function, List<String> argumentTypes) {}

  default void onDropAggregate(String keyspace, String aggregate, List<String> argumentTypes) {}

  default void onJoinCluster(InetAddress endpoint, int port) {}

  default void onLeaveCluster(InetAddress endpoint, int port) {}

  default void onUp(InetAddress endpoint, int port) {}

  default void onDown(InetAddress endpoint, int port) {}

  default void onMove(InetAddress endpoint, int port) {}
}
