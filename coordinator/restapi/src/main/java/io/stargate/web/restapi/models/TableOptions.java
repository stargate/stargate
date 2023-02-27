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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableOptions {

  private Integer defaultTimeToLive;
  private List<ClusteringExpression> clusteringExpression = new ArrayList<>();

  @JsonCreator
  public TableOptions(
      @JsonProperty("defaultTimeToLive") final int defaultTimeToLive,
      @JsonProperty("clusteringExpression") final List<ClusteringExpression> clusteringExpression) {
    this.defaultTimeToLive = defaultTimeToLive;
    this.clusteringExpression = clusteringExpression;
  }

  @ApiModelProperty(
      value =
          "Defines the Time To Live (TTL), which determines the time period (in seconds) to expire data. If the value is >0, TTL is enabled for the entire table and an expiration timestamp is added to each column. The maximum value is 630720000 (20 years). A new TTL timestamp is calculated each time the data is updated and the row is removed after the data expires.")
  public Integer getDefaultTimeToLive() {
    return defaultTimeToLive;
  }

  @ApiModelProperty(
      value =
          "Order rows storage to make use of the on-disk sorting of columns. Specifying order can make query results more efficient. Defaults to ascending if not provided.")
  public List<ClusteringExpression> getClusteringExpression() {
    return clusteringExpression;
  }

  public void setDefaultTimeToLive(int defaultTimeToLive) {
    this.defaultTimeToLive = defaultTimeToLive;
  }

  public void setClusteringExpression(List<ClusteringExpression> clusteringExpression) {
    this.clusteringExpression = clusteringExpression;
  }

  public TableOptions() {}
}
