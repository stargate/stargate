package io.stargate.db.dse.impl;

import io.stargate.db.EventListener;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableId;

public class EventListenerWrapper implements SchemaChangeListener {
  private final EventListener wrapped;

  public EventListenerWrapper(EventListener wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public void onCreateKeyspace(String keyspace) {
    wrapped.onCreateKeyspace(keyspace);
  }

  @Override
  public void onCreateTable(String keyspace, String table) {
    wrapped.onCreateTable(keyspace, table);
  }

  @Override
  public void onCreateView(String keyspace, String view) {
    wrapped.onCreateView(keyspace, view);
  }

  @Override
  public void onCreateType(String keyspace, String type) {
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
  public void onAlterKeyspace(String keyspace) {
    wrapped.onAlterKeyspace(keyspace);
  }

  @Override
  public void onAlterTable(String keyspace, String table, boolean affectsStatements) {
    wrapped.onAlterTable(keyspace, table);
  }

  @Override
  public void onAlterView(String keyspace, String view, boolean affectsStatements) {
    wrapped.onAlterView(keyspace, view);
  }

  @Override
  public void onAlterType(String keyspace, String type) {
    wrapped.onAlterType(keyspace, type);
  }

  @Override
  public void onAlterFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    wrapped.onAlterFunction(keyspace, function, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onAlterAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    wrapped.onAlterAggregate(keyspace, aggregate, AbstractType.asCQLTypeStringList(argumentTypes));
  }

  @Override
  public void onDropKeyspace(String keyspace) {
    wrapped.onDropKeyspace(keyspace);
  }

  @Override
  public void onDropTable(String keyspace, String table, TableId tableId) {
    wrapped.onDropTable(keyspace, table);
  }

  @Override
  public void onDropView(String keyspace, String view, TableId tableId) {
    wrapped.onDropView(keyspace, view);
  }

  @Override
  public void onDropType(String keyspace, String type) {
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
