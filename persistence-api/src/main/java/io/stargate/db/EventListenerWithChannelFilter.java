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
import java.util.Map;
import java.util.function.Predicate;

/**
 * Specialized version of EventListener which accepts a filter of which channels to send the event
 * to.
 */
public interface EventListenerWithChannelFilter extends EventListener {
  void onCreateKeyspace(String keyspace, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateKeyspace(String keyspace) {
    onCreateKeyspace(keyspace, null);
  }

  void onCreateTable(String keyspace, String table, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateTable(String keyspace, String table) {
    onCreateTable(keyspace, table, null);
  }

  default void onCreateView(
      String keyspace, String view, Predicate<Map<String, String>> headerFilter) {
    onCreateTable(keyspace, view, headerFilter);
  }

  @Override
  default void onCreateView(String keyspace, String view) {
    onCreateTable(keyspace, view);
  }

  void onCreateType(String keyspace, String type, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateType(String keyspace, String type) {
    onCreateType(keyspace, type, null);
  }

  void onCreateFunction(
      String keyspace,
      String function,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateFunction(String keyspace, String function, List<String> argumentTypes) {
    onCreateFunction(keyspace, function, argumentTypes, null);
  }

  void onCreateAggregate(
      String keyspace,
      String aggregate,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onCreateAggregate(keyspace, aggregate, argumentTypes, null);
  }

  void onAlterKeyspace(String keyspace, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterKeyspace(String keyspace) {
    onAlterKeyspace(keyspace, null);
  }

  void onAlterTable(String keyspace, String table, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterTable(String keyspace, String table) {
    onAlterTable(keyspace, table, null);
  }

  default void onAlterView(
      String keyspace, String view, Predicate<Map<String, String>> headerFilter) {
    onAlterTable(keyspace, view, headerFilter);
  }

  @Override
  default void onAlterView(String keyspace, String view) {
    onAlterTable(keyspace, view);
  }

  void onAlterType(String keyspace, String type, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterType(String keyspace, String type) {
    onAlterType(keyspace, type, null);
  }

  void onAlterFunction(
      String keyspace,
      String function,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterFunction(String keyspace, String function, List<String> argumentTypes) {
    onAlterFunction(keyspace, function, argumentTypes, null);
  }

  void onAlterAggregate(
      String keyspace,
      String aggregate,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onAlterAggregate(keyspace, aggregate, argumentTypes, null);
  }

  void onDropKeyspace(String keyspace, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropKeyspace(String keyspace) {
    onDropKeyspace(keyspace, null);
  }

  void onDropTable(String keyspace, String table, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropTable(String keyspace, String table) {
    onDropTable(keyspace, table, null);
  }

  default void onDropView(
      String keyspace, String view, Predicate<Map<String, String>> headerFilter) {
    onDropTable(keyspace, view, headerFilter);
  }

  @Override
  default void onDropView(String keyspace, String view) {
    onDropTable(keyspace, view);
  }

  void onDropType(String keyspace, String type, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropType(String keyspace, String type) {
    onDropType(keyspace, type, null);
  }

  void onDropFunction(
      String keyspace,
      String function,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropFunction(String keyspace, String function, List<String> argumentTypes) {
    onDropFunction(keyspace, function, argumentTypes, null);
  }

  void onDropAggregate(
      String keyspace,
      String aggregate,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onDropAggregate(keyspace, aggregate, argumentTypes, null);
  }

  void onJoinCluster(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onJoinCluster(InetAddress endpoint, int port) {
    onJoinCluster(endpoint, port, null);
  }

  void onLeaveCluster(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onLeaveCluster(InetAddress endpoint, int port) {
    onLeaveCluster(endpoint, port, null);
  }

  void onUp(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onUp(InetAddress endpoint, int port) {
    onUp(endpoint, port, null);
  }

  void onDown(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDown(InetAddress endpoint, int port) {
    onDown(endpoint, port, null);
  }

  void onMove(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onMove(InetAddress endpoint, int port) {
    onMove(endpoint, port, null);
  }

  /**
   * This is an event from persistence to close any connections and resources for any clients that
   * match the header filter predicate. This usually means that the persistence layer is unable to
   * handle a client and this lets the transport layer know that its should close any connections to
   * that client.
   *
   * @param headerFilter a predicate used to match affected clients.
   */
  default void onClose(Predicate<Map<String, String>> headerFilter) {
    // No implementation by default when not required
  }
}
