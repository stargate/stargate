package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

// Note: copy from SGv1 TableResponse
@ApiModel(value = "TableResponse", description = "A description of a Table")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2Table {
  private final String name;
  private final String keyspace;
  private final List<Sgv2ColumnDefinition> columnDefinitions;
  private final Sgv2PrimaryKey primaryKey;
  private final Sgv2TableOptions tableOptions;

  @JsonCreator
  public Sgv2Table(
      @JsonProperty("name") final String name,
      @JsonProperty("keyspace") final String keyspace,
      @JsonProperty("columnDefinitions") final List<Sgv2ColumnDefinition> columnDefinitions,
      @JsonProperty("primaryKey") final Sgv2PrimaryKey primaryKey,
      @JsonProperty("tableOptions") final Sgv2TableOptions tableOptions) {
    this.name = name;
    this.keyspace = keyspace;
    this.columnDefinitions = columnDefinitions;
    this.primaryKey = primaryKey;
    this.tableOptions = tableOptions;
  }

  @ApiModelProperty(value = "The name of the table.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(value = "Name of the keyspace the table belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @ApiModelProperty(value = "Definition of columns within the table.")
  public List<Sgv2ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  @ApiModelProperty(
      value = "The definition of the partition and clustering keys that make up the primary key.")
  public Sgv2PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  @ApiModelProperty(value = "Table options that are applied to the table.")
  public Sgv2TableOptions getTableOptions() {
    return tableOptions;
  }
}
