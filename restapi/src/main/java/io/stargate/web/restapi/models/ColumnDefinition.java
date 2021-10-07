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

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnDefinition {

  private String name;
  private String typeDefinition;
  private boolean isStatic;

  public ColumnDefinition(final String name, final String typeDefinition) {
    this(name, typeDefinition, false);
  }

  @JsonCreator
  public ColumnDefinition(
      @JsonProperty("name") final String name,
      @JsonProperty("typeDefinition") final String typeDefinition,
      @JsonProperty("static") final boolean isStatic) {
    this.name = name;
    this.typeDefinition = typeDefinition;
    this.isStatic = isStatic;
  }

  @ApiModelProperty(
      example = "emailaddress",
      required = true,
      value = "Name for the column, which must be unique.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(
      example = "text",
      required = true,
      value = "The type of data allowed in the column.")
  public String getTypeDefinition() {
    return typeDefinition;
  }

  @ApiModelProperty(value = "Denotes whether the column is shared by all rows of a partition.")
  @JsonProperty("static")
  public boolean getIsStatic() {
    return isStatic;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTypeDefinition(String typeDefinition) {
    this.typeDefinition = typeDefinition;
  }

  public void setIsStatic(boolean isStatic) {
    this.isStatic = isStatic;
  }
}
