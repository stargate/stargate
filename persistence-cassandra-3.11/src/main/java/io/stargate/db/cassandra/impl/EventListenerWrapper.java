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
package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.impl.Conversion.toExternal;

import io.stargate.db.EventListener;
import java.net.InetAddress;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.MigrationListener;

public class EventListenerWrapper extends MigrationListener
    implements IEndpointLifecycleSubscriber {
  private EventListener wrapped;

  EventListenerWrapper(EventListener wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public void onJoinCluster(InetAddress endpoint) {
    wrapped.onJoinCluster(toExternal(endpoint));
  }

  @Override
  public void onLeaveCluster(InetAddress endpoint) {
    wrapped.onLeaveCluster(toExternal(endpoint));
  }

  @Override
  public void onUp(InetAddress endpoint) {
    wrapped.onUp(toExternal(endpoint));
  }

  @Override
  public void onDown(InetAddress endpoint) {
    wrapped.onDown(toExternal(endpoint));
  }

  @Override
  public void onMove(InetAddress endpoint) {
    wrapped.onMove(toExternal(endpoint));
  }

  @Override
  public void onCreateKeyspace(String keyspace) {
    wrapped.onCreateKeyspace(keyspace);
  }

  @Override
  public void onCreateColumnFamily(String keyspace, String table) {
    wrapped.onCreateTable(keyspace, table);
  }

  @Override
  public void onCreateView(String keyspace, String view) {
    wrapped.onCreateView(keyspace, view);
  }

  @Override
  public void onCreateUserType(String keyspace, String type) {
    wrapped.onCreateType(keyspace, type);
  }

  @Override
  public void onCreateFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    wrapped.onCreateFunction(keyspace, function, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onCreateAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    wrapped.onCreateAggregate(keyspace, aggregate, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onUpdateKeyspace(String keyspace) {
    wrapped.onAlterKeyspace(keyspace);
  }

  @Override
  public void onUpdateColumnFamily(String keyspace, String table, boolean affectsStatements) {
    wrapped.onAlterTable(keyspace, table);
  }

  @Override
  public void onUpdateView(String keyspace, String view, boolean affectsStatements) {
    wrapped.onAlterView(keyspace, view);
  }

  @Override
  public void onUpdateUserType(String keyspace, String type) {
    wrapped.onAlterType(keyspace, type);
  }

  @Override
  public void onUpdateFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    wrapped.onAlterFunction(keyspace, function, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onUpdateAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    wrapped.onAlterAggregate(keyspace, aggregate, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onDropKeyspace(String keyspace) {
    wrapped.onDropKeyspace(keyspace);
  }

  @Override
  public void onDropColumnFamily(String keyspace, String table) {
    wrapped.onDropTable(keyspace, table);
  }

  @Override
  public void onDropView(String keyspace, String view) {
    wrapped.onDropView(keyspace, view);
  }

  @Override
  public void onDropUserType(String keyspace, String type) {
    wrapped.onDropType(keyspace, type);
  }

  @Override
  public void onDropFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    wrapped.onDropFunction(keyspace, function, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onDropAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    wrapped.onDropAggregate(keyspace, aggregate, AbstractType.asCQLTypeStringList(argumentTypes));
  }
}
