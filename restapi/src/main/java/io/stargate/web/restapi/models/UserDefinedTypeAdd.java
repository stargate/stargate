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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserDefinedTypeAdd implements Serializable {

  @NotNull private String name;

  private boolean ifNotExists = false;

  @JsonProperty("fields")
  @NotNull
  private List<UserDefinedTypeField> fields;

  @ApiModelProperty(required = true, value = "User Defined Type name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(
      value =
          "Determines whether to create a new UDT if an UDT with the same name exists. Attempting to create an existing UDT returns an error unless this option is true.")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @ApiModelProperty(required = true, value = "User Defined Type fields")
  public List<UserDefinedTypeField> getFields() {
    return fields;
  }

  public void setFields(List<UserDefinedTypeField> fields) {
    this.fields = fields;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("fields", fields)
        .add("ifNotExists", ifNotExists)
        .omitNullValues()
        .toString();
  }
}
