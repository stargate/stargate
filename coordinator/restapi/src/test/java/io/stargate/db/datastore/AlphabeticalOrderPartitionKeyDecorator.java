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
package io.stargate.db.datastore;

import io.stargate.db.ComparableKey;
import io.stargate.db.RowDecorator;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A test implementation of {@link RowDecorator} that assumes partition keys to be {@link String}
 * values and sorts them alphabetically using the natural order.
 */
public class AlphabeticalOrderPartitionKeyDecorator implements RowDecorator {

  private final List<Column> partitionKeyColumns;

  public AlphabeticalOrderPartitionKeyDecorator(List<Column> allColumns) {
    this.partitionKeyColumns =
        allColumns.stream().filter(Column::isPartitionKey).collect(Collectors.toList());
  }

  private String decoratedKeyString(Row row) {
    return partitionKeyColumns.stream()
        .map(column -> row.getString(column.name()))
        .collect(Collectors.joining("|"));
  }

  @Override
  public <T extends Comparable<T>> ComparableKey<T> decoratePartitionKey(Row row) {
    String decorated = decoratedKeyString(row);
    //noinspection unchecked
    return (ComparableKey<T>) new ComparableKey<>(String.class, decorated);
  }

  @Override
  public ByteBuffer getComparableBytes(Row row) {
    String decorated = decoratedKeyString(row);
    return ByteBuffer.wrap(decorated.getBytes(StandardCharsets.UTF_8));
  }
}
