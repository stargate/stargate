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

public class PrimaryKey {
  List<String> partitionKey = new ArrayList<>();
  List<String> clusteringKey = new ArrayList<>();

  public PrimaryKey() {}

  public PrimaryKey(final List<String> partitionKey, final List<String> clusteringKey) {
    this.partitionKey = partitionKey;
    this.clusteringKey = clusteringKey;
  }

  public List<String> getPartitionKey() {
    return partitionKey;
  }

  public List<String> getClusteringKey() {
    return clusteringKey;
  }

  public void setPartitionKey(List<String> partitionKey) {
    this.partitionKey = partitionKey;
  }

  public void setClusteringKey(List<String> clusteringKey) {
    this.clusteringKey = clusteringKey;
  }
}