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
    onCreateKeyspace(keyspace, c -> true);
  }

  void onCreateTable(String keyspace, String table, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateTable(String keyspace, String table) {
    onCreateTable(keyspace, table, c -> true);
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
    onCreateType(keyspace, type, c -> true);
  }

  void onCreateFunction(
      String keyspace,
      String function,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateFunction(String keyspace, String function, List<String> argumentTypes) {
    onCreateFunction(keyspace, function, argumentTypes, c -> true);
  }

  void onCreateAggregate(
      String keyspace,
      String aggregate,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onCreateAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onCreateAggregate(keyspace, aggregate, argumentTypes, c -> true);
  }

  void onAlterKeyspace(String keyspace, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterKeyspace(String keyspace) {
    onAlterKeyspace(keyspace, c -> true);
  }

  void onAlterTable(String keyspace, String table, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterTable(String keyspace, String table) {
    onAlterTable(keyspace, table, c -> true);
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
    onAlterType(keyspace, type, c -> true);
  }

  void onAlterFunction(
      String keyspace,
      String function,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterFunction(String keyspace, String function, List<String> argumentTypes) {
    onAlterFunction(keyspace, function, argumentTypes, c -> true);
  }

  void onAlterAggregate(
      String keyspace,
      String aggregate,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onAlterAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onAlterAggregate(keyspace, aggregate, argumentTypes, c -> true);
  }

  void onDropKeyspace(String keyspace, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropKeyspace(String keyspace) {
    onDropKeyspace(keyspace, c -> true);
  }

  void onDropTable(String keyspace, String table, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropTable(String keyspace, String table) {
    onDropTable(keyspace, table, c -> true);
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
    onDropType(keyspace, type, c -> true);
  }

  void onDropFunction(
      String keyspace,
      String function,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropFunction(String keyspace, String function, List<String> argumentTypes) {
    onDropFunction(keyspace, function, argumentTypes, c -> true);
  }

  void onDropAggregate(
      String keyspace,
      String aggregate,
      List<String> argumentTypes,
      Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDropAggregate(String keyspace, String aggregate, List<String> argumentTypes) {
    onDropAggregate(keyspace, aggregate, argumentTypes, c -> true);
  }

  void onJoinCluster(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onJoinCluster(InetAddress endpoint, int port) {
    onJoinCluster(endpoint, port, c -> true);
  }

  void onLeaveCluster(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onLeaveCluster(InetAddress endpoint, int port) {
    onLeaveCluster(endpoint, port, c -> true);
  }

  void onUp(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onUp(InetAddress endpoint, int port) {
    onUp(endpoint, port, c -> true);
  }

  void onDown(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onDown(InetAddress endpoint, int port) {
    onDown(endpoint, port, c -> true);
  }

  void onMove(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter);

  @Override
  default void onMove(InetAddress endpoint, int port) {
    onMove(endpoint, port, c -> true);
  }
}
