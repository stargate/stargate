package io.stargate.db.cassandra.impl;

import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.SchemaChangeListener;

/**
 * Simple abstract {@link SchemaChangeListener} implementation that funnels all the discrete schema
 * changes through a single no-argument callback (the {@link #onSchemaChange()}). For when we want
 * to do "something" on schema changes, but that something doesn't depend on the details of the
 * schema change.
 */
abstract class SimpleCallbackMigrationListener extends SchemaChangeListener {

  abstract void onSchemaChange();

  @Override
  public void onCreateKeyspace(String keyspace) {
    onSchemaChange();
  }

  @Override
  public void onCreateTable(String keyspace, String table) {
    onSchemaChange();
  }

  @Override
  public void onCreateView(String keyspace, String view) {
    onSchemaChange();
  }

  @Override
  public void onCreateType(String keyspace, String type) {
    onSchemaChange();
  }

  @Override
  public void onCreateFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onCreateAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onAlterKeyspace(String keyspace) {
    onSchemaChange();
  }

  @Override
  public void onAlterTable(String keyspace, String table, boolean affectsStatements) {
    onSchemaChange();
  }

  @Override
  public void onAlterView(String keyspace, String view, boolean affectsStatements) {
    onSchemaChange();
  }

  @Override
  public void onAlterType(String keyspace, String type) {
    onSchemaChange();
  }

  @Override
  public void onAlterFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onAlterAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onDropKeyspace(String keyspace) {
    onSchemaChange();
  }

  @Override
  public void onDropTable(String keyspace, String table) {
    onSchemaChange();
  }

  @Override
  public void onDropView(String keyspace, String view) {
    onSchemaChange();
  }

  @Override
  public void onDropType(String keyspace, String type) {
    onSchemaChange();
  }

  @Override
  public void onDropFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onDropAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }
}
