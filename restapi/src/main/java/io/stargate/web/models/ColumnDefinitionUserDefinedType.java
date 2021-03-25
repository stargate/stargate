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
import io.swagger.annotations.ApiModelProperty;

/**
 * Represents a column in a User Defined type like {@link UserDefinedTypeAdd}
 *
 * <p>This class hase been introduced as the existing {@link ColumnDefinition} included attributes
 * relative to Table only (static). Inheritance would still be possible.
 *
 * @since 1.0.16
 * @author Cedrick LUNVEN (@clunven)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnDefinitionUserDefinedType {

  /** (Immutable) unique identifier for the column. */
  private final String name;

  /** (Immutable) type definition like 'map<string,string>'. */
  private final String typeDefinition;

  @JsonCreator
  public ColumnDefinitionUserDefinedType(
      @JsonProperty("name") final String name,
      @JsonProperty("typeDefinition") final String typeDefinition) {
    this.name = name;
    this.typeDefinition = typeDefinition;
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
}
