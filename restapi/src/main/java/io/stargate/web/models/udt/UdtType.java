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
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UdtType {
  private String name;
  private @NotNull CQLType basic;
  private TypeInfo info;

  @ApiModelProperty(value = "The name of the field that is part of the new type")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("basic")
  @ApiModelProperty(
      value =
          "The basic CQL type to be used for the field. If this type is a collection or User Defined Type then an info will also need to be provided for its type. An example of this would be a list of ints.")
  public CQLType getBasic() {
    return basic;
  }

  public void setBasic(CQLType basic) {
    this.basic = basic;
  }

  @JsonProperty("info")
  @ApiModelProperty(
      value =
          "Additional type params like type parameter of a collection (LIST/SET/MAP/TUPLE) and options like frozen.")
  public TypeInfo getInfo() {
    return info;
  }

  public void setInfo(TypeInfo info) {
    this.info = info;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("basic", basic)
        .add("info", info)
        .omitNullValues()
        .toString();
  }
}
