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
package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DocCollection {
  @JsonProperty("name")
  private final String name;

  @JsonProperty("upgradeAvailable")
  private final boolean upgradeAvailable;

  @JsonProperty("upgradeType")
  private final CollectionUpgradeType upgradeType;

  @ApiModelProperty(required = true, value = "The name of the collection.")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @ApiModelProperty(
      value =
          "Whether an upgrade is available. Use the 'upgrade a collection' endpoint with the upgrade type to perform upgrade.")
  @JsonProperty("upgradeAvailable")
  public boolean getUpgradeAvailable() {
    return upgradeAvailable;
  }

  @ApiModelProperty(value = "The upgrade type, if an upgrade is available.")
  @JsonProperty("upgradeType")
  public CollectionUpgradeType getUpgradeType() {
    return upgradeType;
  }

  @JsonCreator
  public DocCollection(
      @JsonProperty("name") String name,
      @JsonProperty("upgradeAvailable") boolean upgradeAvailable,
      @JsonProperty("upgradeType") CollectionUpgradeType upgradeType) {
    this.name = name;
    this.upgradeAvailable = upgradeAvailable;
    this.upgradeType = upgradeType;
  }

  @Override
  public String toString() {
    return String.format(
        "DocCollection(name=%s, upgradeAvailable=%s, upgradeType=%s)",
        name, upgradeAvailable, upgradeType);
  }
}
