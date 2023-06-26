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
package io.stargate.db;

import io.stargate.db.datastore.Row;
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class AbstractRowDecorator implements RowDecorator {

  private final TableName table;
  private final List<String> partitionKeyColumnNames;

  protected AbstractRowDecorator(TableName table, List<String> partitionKeyColumnNames) {
    this.table = table;
    this.partitionKeyColumnNames = partitionKeyColumnNames;
  }

  protected abstract ComparableKey<?> decoratePrimaryKey(Object... rawKeyValues);

  protected Object[] primaryKeyValues(Row row) {
    Object[] pkValues = new Object[partitionKeyColumnNames.size()];
    int idx = 0;
    for (String columnName : partitionKeyColumnNames) {
      ByteBuffer value = row.getBytesUnsafe(columnName);

      if (value == null) {
        throw new IllegalArgumentException(
            String.format(
                "Required value is not present in current row (table: %s, column: %s)",
                table.name(), columnName));
      }

      pkValues[idx++] = value;
    }

    return pkValues;
  }

  @Override
  public <T extends Comparable<T>> ComparableKey<T> decoratePartitionKey(Row row) {
    Object[] pkValues = primaryKeyValues(row);

    //noinspection unchecked
    return (ComparableKey<T>) decoratePrimaryKey(pkValues);
  }
}
