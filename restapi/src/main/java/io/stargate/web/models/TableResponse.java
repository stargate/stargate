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
package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

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
