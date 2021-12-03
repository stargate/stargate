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
package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sgv2.common.cql.builder.CollectionIndexingType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import javax.validation.constraints.NotNull;

@ApiModel("IndexAddRequest")
public class Sgv2IndexAddRequest {

  @NotNull private String column;
  private String name;
  private String type;
  private CollectionIndexingType kind;
  private Map<String, String> options;

  private boolean ifNotExists = false;

  public void setColumn(String column) {
    this.column = column;
  }

  @ApiModelProperty(required = true, value = "Column name")
  public String getColumn() {
    return column;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(
      value =
          "Optional index name. If no name is specified, Cassandra names the index: table_name_column_name_idx.")
  public String getName() {
    return name;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @ApiModelProperty(
      value =
          "Determines whether to create a new index if an index with the same name exists. Attempting to create an existing index returns an error unless this option is true.")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setType(String type) {
    this.type = type;
  }

  @ApiModelProperty(value = "A custom index class name or classpath.")
  public String getType() {
    return type;
  }

  public void setKind(CollectionIndexingType kind) {
    this.kind = kind;
  }

  @JsonProperty("kind")
  @ApiModelProperty(value = "The kind (ENTRIES, KEY, VALUES, FULL) of an index")
  public CollectionIndexingType getKind() {
    return kind;
  }

  @ApiModelProperty(value = "Options passed to a custom index")
  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }
}
