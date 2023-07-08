package io.stargate.db.cassandra.impl;

import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;

/**
 * Simple abstract {@link SchemaChangeListener} implementation that funnels all the discrete schema
 * changes through a single no-argument callback (the {@link #onSchemaChange()}). For when we want
 * to do "something" on schema changes, but that something doesn't depend on the details of the
 * schema change.
 */
abstract class SimpleCallbackMigrationListener implements SchemaChangeListener {

  abstract void onSchemaChange();

  @Override
  public void onCreateKeyspace(KeyspaceMetadata keyspace) {
    onSchemaChange();
  }

  @Override
  public void onCreateTable(TableMetadata table) {
    onSchemaChange();
  }

  @Override
  public void onCreateView(ViewMetadata view) {
    onSchemaChange();
  }

  @Override
  public void onCreateType(UserType type) {
    onSchemaChange();
  }

  @Override
  public void onCreateFunction(UDFunction function) {
    onSchemaChange();
  }

  @Override
  public void onCreateAggregate(UDAggregate aggregate) {
    onSchemaChange();
  }

  @Override
  public void onAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after) {
    onSchemaChange();
  }

  @Override
  public void onPreAlterTable(TableMetadata before, TableMetadata after) {
    // Do we need call for pre-event?
    onSchemaChange();
  }

  @Override
  public void onAlterTable(TableMetadata before, TableMetadata after, boolean affectStatements) {
    onSchemaChange();
  }

  @Override
  public void onPreAlterView(ViewMetadata before, ViewMetadata after) {
    // Do we need call for pre-event?
    onSchemaChange();
  }

  @Override
  public void onAlterView(ViewMetadata before, ViewMetadata after, boolean affectStatements) {
    onSchemaChange();
  }

  @Override
  public void onAlterType(UserType before, UserType after) {
    onSchemaChange();
  }

  @Override
  public void onAlterFunction(UDFunction before, UDFunction after) {
    onSchemaChange();
  }

  @Override
  public void onAlterAggregate(UDAggregate before, UDAggregate after) {
    onSchemaChange();
  }

  @Override
  public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData) {
    onSchemaChange();
  }

  @Override
  public void onDropTable(TableMetadata table, boolean dropData) {
    onSchemaChange();
  }

  @Override
  public void onDropView(ViewMetadata view, boolean dropData) {
    onSchemaChange();
  }

  @Override
  public void onDropType(UserType type) {
    onSchemaChange();
  }

  @Override
  public void onDropFunction(UDFunction function) {
    onSchemaChange();
  }

  @Override
  public void onDropAggregate(UDAggregate aggregate) {
    onSchemaChange();
  }
}
