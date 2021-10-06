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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableResponse {

  private final String name;
  private final String keyspace;
  private final List<ColumnDefinition> columnDefinitions;
  private final PrimaryKey primaryKey;
  private final TableOptions tableOptions;

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

  @ApiModelProperty(value = "The name of the table.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(value = "Name of the keyspace the table belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @ApiModelProperty(value = "Definition of columns within the table.")
  public List<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  @ApiModelProperty(
      value = "The definition of the partition and clustering keys that make up the primary key.")
  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  @ApiModelProperty(value = "Table options that are applied to the table.")
  public TableOptions getTableOptions() {
    return tableOptions;
  }
}
