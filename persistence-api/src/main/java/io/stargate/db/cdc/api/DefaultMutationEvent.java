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
package io.stargate.db.cdc.api;

import io.stargate.db.schema.Table;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class DefaultMutationEvent implements MutationEvent {

  private final Table table;
  private final OptionalInt ttl;
  private final OptionalLong timestamp;
  private final List<CellValue> partitionKeys;
  private final List<Cell> clusteringKeys;
  private final MutationEventType mutationEventType;
  private final List<Cell> cells;

  public DefaultMutationEvent(
      Table table,
      OptionalInt ttl,
      OptionalLong timestamp,
      List<CellValue> partitionKeys,
      List<Cell> clusteringKeys,
      List<Cell> cells,
      MutationEventType mutationEventType) {
    this.table = table;
    this.ttl = ttl;
    this.timestamp = timestamp;
    this.partitionKeys = partitionKeys;
    this.clusteringKeys = clusteringKeys;
    this.cells = cells;
    this.mutationEventType = mutationEventType;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public OptionalInt ttl() {
    return ttl;
  }

  @Override
  public OptionalLong timestamp() {
    return timestamp;
  }

  @Override
  public List<CellValue> getPartitionKeys() {
    return partitionKeys;
  }

  @Override
  public List<Cell> getClusteringKeys() {
    return clusteringKeys;
  }

  @Override
  public List<Cell> getCells() {
    return cells;
  }

  @Override
  public MutationEventType mutationEventType() {
    return mutationEventType;
  }
}
