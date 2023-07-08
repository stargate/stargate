package io.stargate.db.cassandra.impl;

import io.stargate.db.EventListener;
import java.util.List;

import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;

public class EventListenerWrapper implements SchemaChangeListener {
  private final EventListener wrapped;

  EventListenerWrapper(EventListener wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public void onCreateKeyspace(KeyspaceMetadata keyspace) {
    wrapped.onCreateKeyspace(keyspace.name);
  }

  @Override
  public void onCreateTable(TableMetadata table) {
    wrapped.onCreateTable(table.keyspace, table.name);
  }

  @Override
  public void onCreateView(ViewMetadata view) {
    wrapped.onCreateView(view.keyspace(), view.name());
  }

  @Override
  public void onCreateType(UserType type) {
    wrapped.onCreateType(type.keyspace, type.getNameAsString());
  }

  @Override
  public void onCreateFunction(UDFunction function) {
    wrapped.onCreateFunction(function.name().keyspace, function.name().name, AbstractType.asCQLTypeStringList(function.argTypes()));
  }

  @Override
  public void onCreateAggregate(UDAggregate aggregate) {
    wrapped.onCreateAggregate(aggregate.name().keyspace, aggregate.name().name, AbstractType.asCQLTypeStringList(aggregate.argTypes()));
  }

  @Override
  public void onAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after) {
    wrapped.onAlterKeyspace(before.name);
  }

  @Override
  public void onPreAlterTable(TableMetadata before, TableMetadata after) {
    // What to do here?
  }

  @Override
  public void onAlterTable(TableMetadata before, TableMetadata after, boolean affectStatements) {
    wrapped.onAlterTable(before.keyspace, before.name);
  }

  @Override
  public void onPreAlterView(ViewMetadata before, ViewMetadata after) {
    // What to do here?
  }

  @Override
  public void onAlterView(ViewMetadata before, ViewMetadata after, boolean affectStatements) {
    wrapped.onAlterView(before.keyspace(), before.name());
  }

  @Override
  public void onAlterType(UserType before, UserType after) {
    wrapped.onAlterType(before.keyspace, before.getNameAsString());
  }

  @Override
  public void onAlterFunction(UDFunction before, UDFunction after) {
    wrapped.onAlterFunction(before.name().keyspace, before.name().name,
            AbstractType.asCQLTypeStringList(before.argTypes()));
  }

  @Override
  public void onAlterAggregate(UDAggregate before, UDAggregate after) {
    wrapped.onAlterAggregate(before.name().keyspace, before.name().name,
            AbstractType.asCQLTypeStringList(before.argTypes()));
  }

  @Override
  public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData) {
    wrapped.onDropKeyspace(keyspace.name);
  }

  @Override
  public void onDropTable(TableMetadata table, boolean dropData) {
    wrapped.onDropTable(table.keyspace, table.name);
  }

  @Override
  public void onDropView(ViewMetadata view, boolean dropData) {
    wrapped.onDropView(view.keyspace(), view.name());
  }

  @Override
  public void onDropType(UserType type) {
    wrapped.onDropType(type.keyspace, type.getNameAsString());
  }

  @Override
  public void onDropFunction(UDFunction function) {
    wrapped.onDropFunction(function.name().keyspace, function.name().name,
            AbstractType.asCQLTypeStringList(function.argTypes()));
  }

  @Override
  public void onDropAggregate(UDAggregate aggregate) {
    wrapped.onDropAggregate(aggregate.name().keyspace, aggregate.name().name,
            AbstractType.asCQLTypeStringList(aggregate.argTypes()));
  }
}
