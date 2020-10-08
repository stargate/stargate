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
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Keyspace {
  @JsonProperty("name")
  String name;

  @JsonProperty("datacenters")
  List<Datacenter> datacenters;

  @ApiModelProperty(
      value =
          "The datacenters within a keyspace. Only applies for those keyspaces created with NetworkTopologyStrategy.")
  public List<Datacenter> getDatacenters() {
    return datacenters;
  }

  public void setDatacenters(List<Datacenter> datacenters) {
    this.datacenters = datacenters;
  }

  @ApiModelProperty(required = true, value = "The name of the keyspace.")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public Keyspace setName(String name) {
    this.name = name;
    return this;
  }

  @JsonCreator
  public Keyspace(
      @JsonProperty("name") String name,
      @JsonProperty("datacenters") List<Datacenter> datacenters) {
    this.name = name;
    this.datacenters = datacenters;
  }
}
