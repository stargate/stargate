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
package io.stargate.web.docsapi.models.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.db.query.builder.Replication;
import io.swagger.annotations.ApiModelProperty;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/** The DTO for the namespace creation. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateNamespace {

  @JsonProperty("name")
  @NotNull(message = "`name` is required to create a namespace")
  @NotBlank(message = "`name` is required to create a namespace")
  private final String name;

  @JsonProperty("replicas")
  @Min(value = 1, message = "minimum amount of `replicas` for `SimpleStrategy` is one")
  private final Integer replicas;

  @JsonProperty("datacenters")
  @Valid
  private final Collection<Datacenter> datacenters;

  @JsonCreator
  public CreateNamespace(
      @JsonProperty("name") String name,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("datacenters") Collection<Datacenter> datacenters) {
    this.name = name;
    this.replicas = replicas;
    this.datacenters = datacenters;
  }

  @JsonIgnore
  public Replication getReplication() {
    if (null == datacenters || datacenters.isEmpty()) {
      int replicas = Optional.ofNullable(this.replicas).orElse(1);
      return Replication.simpleStrategy(replicas);
    } else {
      Map<String, Integer> datacenterMap =
          datacenters.stream()
              .collect(
                  Collectors.toMap(
                      Datacenter::getName, dc -> Optional.ofNullable(dc.getReplicas()).orElse(3)));
      return Replication.networkTopologyStrategy(datacenterMap);
    }
  }

  @ApiModelProperty(required = true, value = "The name of the namespace.", example = "myspace")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @ApiModelProperty(
      value =
          "The amount of replicas to use with the `SimpleStrategy`. Ignored if `datacenters` is set. Defaults to 1.",
      example = "1")
  @JsonProperty("replicas")
  public Integer getReplicas() {
    return replicas;
  }

  @ApiModelProperty(value = "The datacenters description for the `NetworkTopologyStrategy`.")
  @JsonProperty("datacenters")
  public Collection<Datacenter> getDatacenters() {
    return datacenters;
  }

  @Override
  public String toString() {
    return String.format(
        "CreateNamespace(name=%s,replicas=%s,datacenters=%s)", name, replicas, datacenters);
  }

  public static final class Datacenter {

    @JsonProperty("name")
    @NotNull(message = "a datacenter `name` is required when using `NetworkTopologyStrategy`")
    @NotBlank(message = "a datacenter `name` is required when using `NetworkTopologyStrategy`")
    private final String name;

    @JsonProperty("replicas")
    @Min(value = 1, message = "minimum amount of `replicas` for a datacenter is one")
    private final Integer replicas;

    @JsonCreator
    public Datacenter(
        @JsonProperty("name") String name, @JsonProperty("replicas") Integer replicas) {
      this.name = name;
      this.replicas = replicas;
    }

    @ApiModelProperty(required = true, value = "The name of the datacenter.", example = "dc1")
    @JsonProperty("name")
    public String getName() {
      return name;
    }

    @ApiModelProperty(
        value = "The amount of replicas for the given `datacenter`. Defaults to 3.",
        example = "3")
    @JsonProperty("replicas")
    public Integer getReplicas() {
      return replicas;
    }

    @Override
    public String toString() {
      return String.format("CreateNamespace.Datacenter(name=%s,replicas=%s)", name, replicas);
    }
  }
}
