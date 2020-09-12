package io.stargate.web.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableResponse {
  String name;
  String keyspace;
  List<ColumnDefinition> columnDefinitions;
  PrimaryKey primaryKey;
  TableOptions tableOptions;

  @JsonCreator
  public TableResponse(
          @JsonProperty("name") final String name,
          @JsonProperty("keyspace") final String keyspace,
          @JsonProperty("columnDefinitions") final List<ColumnDefinition> columnDefinitions,
          @JsonProperty("primaryKey") final PrimaryKey primaryKey,
          @JsonProperty("tableOptions") final TableOptions tableOptions) {
    this.name = name;
    this.keyspace = keyspace;
    this.columnDefinitions = columnDefinitions;
    this.primaryKey = primaryKey;
    this.tableOptions = tableOptions;
  }

  public String getName() {
    return name;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public List<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public TableOptions getTableOptions() {
    return tableOptions;
  }
}