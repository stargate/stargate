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
package io.stargate.web.models.udt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TypeInfo {
  private String name;
  private boolean frozen;
  private @NotNull List<UdtType> subTypes;

  @ApiModelProperty(
      value = "The optional name of the type if the basic type is a UDT itself, for example")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(
      value =
          "Denotes whether or not the type should be defined as frozen. Using frozen means any upsert overwrites the entire value that is treated like a Blob.")
  public boolean isFrozen() {
    return frozen;
  }

  public void setFrozen(boolean frozen) {
    this.frozen = frozen;
  }

  @JsonProperty("typeParams")
  @ApiModelProperty(
      value =
          "The CQL sub-type of the parent type. This is to be used when the parent type is a collection (list, tuple, map, or set), a TUPLE or a UDT.")
  public List<UdtType> getSubTypes() {
    return subTypes;
  }

  public void setSubTypes(List<UdtType> subTypes) {
    this.subTypes = subTypes;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("frozen", frozen)
        .add("typeParams", subTypes)
        .omitNullValues()
        .toString();
  }
}
