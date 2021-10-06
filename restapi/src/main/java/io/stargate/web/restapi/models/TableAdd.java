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
package io.stargate.web.restapi.models;

import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

public class TableAdd {

  @NotNull private String name;
  @NotNull private PrimaryKey primaryKey;
  @NotNull private List<ColumnDefinition> columnDefinitions;

  boolean ifNotExists = false;
  TableOptions tableOptions = new TableOptions();

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
  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(PrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  @ApiModelProperty(
      required = true,
      value = "Definition of columns that belong to the table to be added.")
  public List<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  @ApiModelProperty(value = "The set of table options to apply to the table when creating.")
  public TableOptions getTableOptions() {
    return tableOptions;
  }

  public void setTableOptions(TableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }
}
