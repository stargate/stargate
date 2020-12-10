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

import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import io.stargate.db.query.*;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class MutationEventBuilder {
  private OptionalInt ttl;
  private OptionalLong timestamp;
  private Table table;
  private List<CellValue> partitionKeys;
  private List<CellValue> clusteringKeys;
  private MutationEventType mutationEventType;
  private List<Cell> cells;

  public MutationEventBuilder withTTL(OptionalInt ttl) {
    this.ttl = ttl;
    return this;
  }

  public MutationEventBuilder withTimestamp(OptionalLong timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public MutationEventBuilder withTable(Table table) {
    this.table = table;
    return this;
  }

  public MutationEventBuilder withPartitionKeys(Set<PartitionKey> partitionKeys) {
    this.partitionKeys = toCellValue(partitionKeys);
    return this;
  }

  public MutationEventBuilder withClusteringKeys(Collection<PrimaryKey> clusteringKeys) {
    this.clusteringKeys =
        toCellValue(clusteringKeys).stream()
            .filter(v -> v.getColumn().isClusteringKey())
            .collect(Collectors.toList());
    return this;
  }

  public MutationEventBuilder withMutationEventType(MutationEventType mutationEventType) {
    this.mutationEventType = mutationEventType;
    return this;
  }

  private MutationEventBuilder withCells(List<Modification> modifications) {
    this.cells = toCell(modifications);
    return this;
  }

  private List<Cell> toCell(Collection<Modification> modifications) {
    return modifications.stream()
        .map(
            c -> {
              Optional<TypedValue> value = Optional.ofNullable(c.value());
              return new DefaultCell(
                  value.map(TypedValue::bytes).orElse(null),
                  value.map(TypedValue::javaValue).orElse(null),
                  c.entity().getColumn(),
                  0, // not handled yet
                  c.operation());
            })
        .collect(Collectors.toList());
  }

  private List<CellValue> toCellValue(Collection<? extends SchemaKey> partitionKeys) {
    Stream<TypedValue> values =
        partitionKeys.stream().map(SchemaKey::allValues).flatMap(Collection::stream);
    Stream<Column> columns =
        partitionKeys.stream().map(SchemaKey::allColumns).flatMap(Collection::stream);

    return mergeValuesAndColumns(values, columns);
  }

  private List<CellValue> mergeValuesAndColumns(Stream<TypedValue> values, Stream<Column> columns) {
    return Streams.zip(
            values,
            columns,
            (BiFunction<TypedValue, Column, CellValue>)
                (typedValue, column) ->
                    new DefaultCellValue(typedValue.bytes(), typedValue.javaValue(), column))
        .collect(Collectors.toList());
  }

  private MutationEventType toType(BoundDMLQuery boundDMLQuery) {
    if (boundDMLQuery.type().equals(QueryType.DELETE)) {
      return MutationEventType.DELETE;
    } else {
      return MutationEventType.UPDATE;
    }
  }

  @Nonnull
  public MutationEvent build() {
    return new DefaultMutationEvent(
        table, ttl, timestamp, partitionKeys, clusteringKeys, cells, mutationEventType);
  }

  public MutationEventBuilder fromBoundDMLQuery(BoundDMLQuery boundDMLQuery) {
    if (boundDMLQuery.rowsUpdated().isRanges()) {
      throw new UnsupportedOperationException("ranges are not yet supported in CDC");
      // https://github.com/stargate/stargate/issues/492
    }

    List<Modification> nonPkCkColumns =
        boundDMLQuery.modifications().stream()
            .filter(c -> !c.entity().getColumn().isPrimaryKeyComponent())
            .collect(Collectors.toList());

    withTTL(boundDMLQuery.ttl())
        .withTimestamp(boundDMLQuery.timestamp())
        .withTable(boundDMLQuery.table())
        .withPartitionKeys(boundDMLQuery.rowsUpdated().asKeys().partitionKeys())
        .withClusteringKeys(boundDMLQuery.rowsUpdated().asKeys().primaryKeys())
        .withMutationEventType(toType(boundDMLQuery))
        .withCells(nonPkCkColumns)
        .build();
    return this;
  }
}
