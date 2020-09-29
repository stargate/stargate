package io.stargate.db.cassandra.impl;

import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.MigrationListener;

/**
 * Simple abstract {@link MigrationListener} implementation that funnels all the discrete schema
 * changes through a single no-argument callback (the {@link #onSchemaChange()}). For when we want
 * to do "something" on schema changes, but that something doesn't depend on the details of the
 * schema change.
 */
abstract class SimpleCallbackMigrationListener extends MigrationListener {

  abstract void onSchemaChange();

  @Override
  public void onCreateKeyspace(String keyspace) {
    onSchemaChange();
  }

  @Override
  public void onCreateColumnFamily(String keyspace, String table) {
    onSchemaChange();
  }

  @Override
  public void onCreateView(String keyspace, String view) {
    onSchemaChange();
  }

  @Override
  public void onCreateUserType(String keyspace, String type) {
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
  public void onUpdateKeyspace(String keyspace) {
    onSchemaChange();
  }

  @Override
  public void onUpdateColumnFamily(String keyspace, String table, boolean affectsStatements) {
    onSchemaChange();
  }

  @Override
  public void onUpdateView(String keyspace, String view, boolean affectsStatements) {
    onSchemaChange();
  }

  @Override
  public void onUpdateUserType(String keyspace, String type) {
    onSchemaChange();
  }

  @Override
  public void onUpdateFunction(
      String keyspace, String function, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onUpdateAggregate(
      String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
    onSchemaChange();
  }

  @Override
  public void onDropKeyspace(String keyspace) {
    onSchemaChange();
  }

  @Override
  public void onDropColumnFamily(String keyspace, String table) {
    onSchemaChange();
  }

  @Override
  public void onDropView(String keyspace, String view) {
    onSchemaChange();
  }

  @Override
  public void onDropUserType(String keyspace, String type) {
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
