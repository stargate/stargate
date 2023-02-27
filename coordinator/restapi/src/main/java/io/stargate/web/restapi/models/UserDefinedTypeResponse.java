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

/** Represent a user defined type at api level. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserDefinedTypeResponse {

  private final String name;

  private final String keyspace;

  private final List<UserDefinedTypeField> fields;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public UserDefinedTypeResponse(
      @JsonProperty("name") final String name,
      @JsonProperty("keyspace") final String keyspace,
      @JsonProperty("fields") final List<UserDefinedTypeField> fields) {
    this.name = name;
    this.keyspace = keyspace;
    this.fields = fields;
  }

  @ApiModelProperty(value = "The name of the user defined type.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(value = "Name of the keyspace the user defined type belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @ApiModelProperty(value = "Definition of columns within the user defined type.")
  public List<UserDefinedTypeField> getFields() {
    return fields;
  }
}
