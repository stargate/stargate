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
package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2Keyspace {
  protected String name;

  protected List<Datacenter> datacenters;

  protected Sgv2Keyspace() {} // for deserializer

  public Sgv2Keyspace(String name) {
    this.name = name;
  }

  @ApiModelProperty(required = true, value = "The name of the keyspace.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(
      value =
          "The datacenters within a keyspace. Only applies for those keyspaces created with NetworkTopologyStrategy.")
  public List<Datacenter> getDatacenters() {
    return datacenters;
  }

  public void addDatacenter(String name, int replicas) {
    if (datacenters == null) {
      datacenters = new ArrayList<>();
    }
    datacenters.add(new Datacenter(name, replicas));
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Datacenter {
    private String name;
    private int replicas;

    @JsonCreator
    public Datacenter(@JsonProperty("name") String name, @JsonProperty("replicas") int replicas) {
      this.name = name;
      this.replicas = replicas;
    }

    @ApiModelProperty(required = true, value = "The name of the datacenter.")
    public String getName() {
      return name;
    }

    public Datacenter setName(String name) {
      this.name = name;
      return this;
    }

    @ApiModelProperty(
        required = true,
        value =
            "The number of replicas in the datacenter. In other words, the number of copies of each row in the datacenter.")
    public int getReplicas() {
      return replicas;
    }

    public Datacenter setReplicas(int replicas) {
      this.replicas = replicas;
      return this;
    }
  }
}
