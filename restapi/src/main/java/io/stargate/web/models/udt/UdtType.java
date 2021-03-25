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
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UdtType {
  private String name;
  private @NotNull CQLType basic;
  private UdtInfo info;

  @ApiModelProperty(value = "return the field name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("basic")
  @ApiModelProperty(value = "return the field type.")
  public CQLType getBasic() {
    return basic;
  }

  public void setBasic(CQLType basic) {
    this.basic = basic;
  }

  @JsonProperty("info")
  @ApiModelProperty(value = "return type info.")
  public UdtInfo getInfo() {
    return info;
  }

  public void setInfo(UdtInfo info) {
    this.info = info;
  }

  @Override
  public String toString() {
    return String.format("name: %s, basic: %s, info: %s", name, basic, info);
  }
}
