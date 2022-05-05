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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.common.model;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A wrapper around the protobuf-generated {@link Row}, to add the column metadata. */
@org.immutables.value.Value.Immutable
public interface RowWrapper {

  /** @return The list of {@link ColumnSpec} for the row. */
  @org.immutables.value.Value.Parameter
  List<ColumnSpec> columns();

  /** @return The actual row. */
  @org.immutables.value.Value.Parameter
  Row row();

  /** @return If column exists in the row. */
  default boolean columnExists(String columnName) {
    return firstIndexOf(columnName) >= 0;
  }

  /**
   * @return If value of the column is <code>null</code>.
   * @throws IllegalArgumentException If column does not exist.
   */
  default boolean isNull(String columnName) {
    return getValue(columnName).hasNull();
  }

  /**
   * @return Value of the column as string.
   * @throws IllegalArgumentException If column does not exist.
   */
  default String getString(String columnName) {
    return Values.string(getValue(columnName));
  }

  /**
   * @return Value of the column as double.
   * @throws IllegalArgumentException If column does not exist.
   */
  default Double getDouble(String columnName) {
    return Values.double_(getValue(columnName));
  }

  /**
   * @return Value of the column as byte.
   * @throws IllegalArgumentException If column does not exist.
   */
  default byte getByte(String columnName) {
    return Values.tinyint(getValue(columnName));
  }

  /**
   * @return Value of the column as boolean.
   * @throws IllegalArgumentException If column does not exist.
   */
  default Boolean getBoolean(String columnName) {
    return Values.bool(getValue(columnName));
  }

  /** @return Map of column names to index in the {@link #row()}. */
  @org.immutables.value.Value.Derived
  default Map<String, Integer> columnIndexMap() {
    Map<String, Integer> colIndexes = new HashMap<>();
    for (int index = 0; index < columns().size(); index++) {
      QueryOuterClass.ColumnSpec col = columns().get(index);
      colIndexes.put(col.getName(), index);
    }
    return colIndexes;
  }

  private Value getValue(String columnName) {
    int i = firstIndexOf(columnName);
    if (i < 0) {
      throw new IllegalArgumentException(String.format("Column '%s' does not exist", columnName));
    }
    return row().getValues(i);
  }

  /** @return the index, or <0 if the column does not exist. */
  private int firstIndexOf(String columnName) {
    Integer index = columnIndexMap().get(columnName);
    return index != null ? index : -1;
  }
}
