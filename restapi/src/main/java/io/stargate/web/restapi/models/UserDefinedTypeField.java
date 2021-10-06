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

import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

/** Represents a column in a User Defined type like {@link UserDefinedTypeAdd} */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserDefinedTypeField {

  private final String name;
  private final String typeDefinition;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public UserDefinedTypeField(
      @JsonProperty("name") final String name,
      @JsonProperty("typeDefinition") final String typeDefinition) {
    this.name = name;
    this.typeDefinition = typeDefinition;
  }

  @ApiModelProperty(
      example = "emailaddress",
      required = true,
      value = "Name for the type, which must be unique.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(
      example = "text",
      required = true,
      value = "A valid type of data (e.g, text, int, etc) allowed in the type.")
  public String getTypeDefinition() {
    return typeDefinition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("fields", typeDefinition)
        .omitNullValues()
        .toString();
  }
}
