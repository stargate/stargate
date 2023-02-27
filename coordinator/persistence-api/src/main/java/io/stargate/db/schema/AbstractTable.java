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
package io.stargate.db.schema;

import static io.stargate.db.schema.Column.Kind.Clustering;
import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static io.stargate.db.schema.Column.Kind.Regular;
import static io.stargate.db.schema.Column.Kind.Static;
import static java.lang.String.format;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Cassandra tables and materialized views are queried in roughly the same way. This class abstracts
 * Cassandra's map of (partitionKey, clusteringKey) -> fields into an Expression based query.
 */
public abstract class AbstractTable implements Index, QualifiedSchemaEntity {
  private static final long serialVersionUID = -5320339139947924742L;

  public abstract List<Column> columns();

  public abstract String comment();

  public abstract int ttl();

  @Value.Lazy
  Map<String, Column> columnMap() {
    return columns().stream().collect(Collectors.toMap(Column::name, Function.identity()));
  }

  @Value.Lazy
  public List<Column> partitionKeyColumns() {
    return ImmutableList.copyOf(
        columns().stream().filter(c -> c.kind() == PartitionKey).collect(Collectors.toList()));
  }

  // for clustering keys, order matters
  @Value.Lazy
  public List<Column> clusteringKeyColumns() {
    return ImmutableList.copyOf(
        columns().stream().filter(c -> c.kind() == Clustering).collect(Collectors.toList()));
  }

  @Value.Lazy
  public List<Column> primaryKeyColumns() {
    return new ImmutableList.Builder<Column>()
        .addAll(partitionKeyColumns())
        .addAll(clusteringKeyColumns())
        .build();
  }

  @Value.Lazy
  public List<Column> regularAndStaticColumns() {
    return ImmutableList.copyOf(
        columns().stream()
            .filter(c -> c.kind() == Regular || c.kind() == Static)
            .collect(Collectors.toList()));
  }

  @Value.Lazy
  public Set<Column> getRequiredIndexColumns() {
    return ImmutableSet.<Column>builder().addAll(partitionKeyColumns()).build();
  }

  @Value.Lazy
  public Set<Column> getOptionalIndexColumns() {
    return ImmutableSet.<Column>builder().addAll(clusteringKeyColumns()).build();
  }

  /**
   * The index of the provided primary key column in the primary key.
   *
   * <p>For instance, if the table primary key is {@code PRIMARY KEY (a, b, c, d)}, then this method
   * will return 0 for column "a", 1 for column "b", etc...
   *
   * @throws IllegalArgumentException if the provided column is not a primary key column on this
   *     table.
   */
  public int primaryKeyColumnIndex(Column column) {
    // TODO: Column could maintain that index for primary keys, which would save us this.
    //  Maybe not a big deal in practice since primary key definitions can only get so long.
    List<Column> pks = primaryKeyColumns();
    for (int i = 0; i < pks.size(); i++) {
      if (column.name().equals(pks.get(i).name())) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        format("Column %s is not a primary key column of %s.%s", column, cqlKeyspace(), cqlName()));
  }

  public Column column(String name) {
    if (Column.TTL.name().equals(name)) {
      return Column.TTL;
    }
    if (Column.TIMESTAMP.name().equals(name)) {
      return Column.TIMESTAMP;
    }
    return columnMap().get(name);
  }

  public Column existingColumn(String name) {
    Column column = column(name);
    if (column == null) {
      throw new IllegalArgumentException(
          format(
              "Cannot find column %s in table %s.%s",
              ColumnUtils.maybeQuote(name), cqlKeyspace(), cqlName()));
    }
    return column;
  }
}
