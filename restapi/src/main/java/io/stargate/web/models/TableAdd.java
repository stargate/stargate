package io.stargate.web.models;

import java.util.List;

import javax.validation.constraints.NotNull;

public class TableAdd {
  @NotNull
  String name;
  @NotNull
  PrimaryKey primaryKey;
  @NotNull
  List<ColumnDefinition> columnDefinitions;

  boolean ifNotExists = false;
  TableOptions tableOptions = new TableOptions();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(PrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  public TableOptions getTableOptions() {
    return tableOptions;
  }

  public void setTableOptions(TableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }
  
}