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
import java.util.List;
import java.util.stream.Collectors;

public class AlphabeticalOrderPartitionKeyDecorator implements RowDecorator {

  private final List<Column> partitionKeyColumns;

  public AlphabeticalOrderPartitionKeyDecorator(List<Column> allColumns) {
    this.partitionKeyColumns =
        allColumns.stream().filter(Column::isPartitionKey).collect(Collectors.toList());
  }

  @Override
  public <T extends Comparable<T>> ComparableKey<T> decoratePartitionKey(Row row) {
    StringBuilder decorated = new StringBuilder();
    for (Column column : partitionKeyColumns) {
      String value = row.getString(column.name());
      decorated.append(value);
      decorated.append("|");
    }
    //noinspection unchecked
    return (ComparableKey<T>) new ComparableKey<>(String.class, decorated.toString());
  }
}
