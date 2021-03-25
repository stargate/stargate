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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.MoreObjects;
import io.stargate.web.models.udt.UdtType;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TypeAdd {
  private @NotNull String name;
  private boolean ifNotExists;
  private @NotNull List<UdtType> fields;

  @ApiModelProperty(required = true, value = "The name of the type.")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(
      value =
          "Determines whether to create a new type if a type with the same name exists. Attempting to create an existing type returns an error unless this option is true.")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @ApiModelProperty(required = true, value = "The fields defined for this type.")
  public List<UdtType> getFields() {
    return fields;
  }

  public void setFields(List<UdtType> fields) {
    this.fields = fields;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fields", fields)
        .add("ifNotExists", ifNotExists)
        .omitNullValues()
        .toString();
  }
}
