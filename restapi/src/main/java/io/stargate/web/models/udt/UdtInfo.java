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
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UdtInfo {
  private String name;
  private boolean frozen;
  private @NotNull List<UdtType> subTypes;

  @ApiModelProperty(value = "return the name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @ApiModelProperty(value = "return true if is frozen")
  public boolean isFrozen() {
    return frozen;
  }

  public void setFrozen(boolean frozen) {
    this.frozen = frozen;
  }

  @JsonProperty("subTypes")
  @ApiModelProperty(value = "return subtypes")
  public List<UdtType> getSubTypes() {
    return subTypes;
  }

  public void setSubTypes(List<UdtType> subTypes) {
    this.subTypes = subTypes;
  }

  @Override
  public String toString() {
    return "{ name: " + name + ", frozen: " + frozen + ", subTypes: " + subTypes + "}";
  }
}
