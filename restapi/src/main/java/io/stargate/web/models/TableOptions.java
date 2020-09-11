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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableOptions {
  Integer defaultTimeToLive;
  List<ClusteringExpression> clusteringExpression = new ArrayList<>();

  @JsonCreator
  public TableOptions(
          @JsonProperty("defaultTimeToLive") final int defaultTimeToLive,
          @JsonProperty("clusteringExpression") final List<ClusteringExpression> clusteringExpression) {
    this.defaultTimeToLive = defaultTimeToLive;
    this.clusteringExpression = clusteringExpression;
  }

  public Integer getDefaultTimeToLive() {
    return defaultTimeToLive;
  }

  public List<ClusteringExpression> getClusteringExpression() {
    return clusteringExpression;
  }

  public void setDefaultTimeToLive(int defaultTimeToLive) {
    this.defaultTimeToLive = defaultTimeToLive;
  }

  public void setClusteringExpression(List<ClusteringExpression> clusteringExpression) {
    this.clusteringExpression = clusteringExpression;
  }

  public TableOptions() {
  }
}