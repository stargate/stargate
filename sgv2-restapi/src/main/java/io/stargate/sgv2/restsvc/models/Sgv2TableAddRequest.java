package io.stargate.sgv2.restsvc.models;

import io.swagger.annotations.ApiModelProperty;
import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;

// Copied from SGv1 "TableAdd"
public class Sgv2TableAddRequest {
  @NotNull private String name;
  @NotNull private Sgv2Table.PrimaryKey primaryKey;
  @NotNull private List<Sgv2ColumnDefinition> columnDefinitions;

  boolean ifNotExists = false;

  Sgv2Table.TableOptions tableOptions = new Sgv2Table.TableOptions();

  @ApiModelProperty(required = true, value = "The name of the table to add.")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(
      value =
          "Determines whether to create a new table if a table with the same name exists. Attempting to create an existing table returns an error unless this option is true.")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @ApiModelProperty(
      required = true,
      value =
          "The primary key definition of the table, consisting of partition and clustering keys.")
  public Sgv2Table.PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(Sgv2Table.PrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  @ApiModelProperty(
      required = true,
      value = "Definition of columns that belong to the table to be added.")
  public List<Sgv2ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  public void setColumnDefinitions(List<Sgv2ColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  @ApiModelProperty(value = "The set of table options to apply to the table when creating.")
  public Sgv2Table.TableOptions getTableOptions() {
    return tableOptions;
  }

  public void setTableOptions(Sgv2Table.TableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }

  // // // Convenience access

  public List<Sgv2Table.ClusteringExpression> findClusteringExpressions() {
    if (tableOptions != null) {
      List<Sgv2Table.ClusteringExpression> clustering = tableOptions.getClusteringExpression();
      if (clustering != null) {
        return clustering;
      }
    }
    return Collections.emptyList();
  }
}
