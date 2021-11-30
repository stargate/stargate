/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.sgv2.common.cql.builder;

import static java.lang.String.format;

import java.util.Map;

/**
 * Represents a keyspace replication string.
 *
 * <p>So something like "{ 'class': 'SimpleStrategy', 'replication_factor': 1}".
 */
public class Replication {
  private final String replication;

  private Replication(String replication) {
    this.replication = replication;
  }

  public static Replication simpleStrategy(int replicationFactor) {
    return new Replication(
        format("{ 'class': 'SimpleStrategy', 'replication_factor': %d }", replicationFactor));
  }

  public static Replication networkTopologyStrategy(
      Map<String, Integer> dataCenterReplicationFactors) {
    StringBuilder sb = new StringBuilder();
    sb.append("{ 'class' : 'NetworkTopologyStrategy'");
    for (Map.Entry<String, Integer> entry : dataCenterReplicationFactors.entrySet()) {
      String dcName = entry.getKey();
      int dcReplication = entry.getValue();
      sb.append(", '").append(dcName).append("': ").append(dcReplication);
    }
    sb.append(" }");
    return new Replication(sb.toString());
  }

  @Override
  public String toString() {
    return replication;
  }
}
