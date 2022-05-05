/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.stargate.sgv2.docsapi.service.util;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import java.util.List;
import java.util.Objects;

/** A wrapper around the protobuf-generated {@link Row}, to add the column metadata. */
public class ExtendedRow {

  private final List<ColumnSpec> columns;
  private final Row row;

  public ExtendedRow(List<ColumnSpec> columns, Row row) {
    this.columns = columns;
    this.row = row;
  }

  public boolean columnExists(String columnName) {
    return columns.stream().anyMatch(c -> Objects.equals(c.getName(), columnName));
  }

  public boolean isNull(String columnName) {
    return getValue(columnName).hasNull();
  }

  public String getString(String columnName) {
    return Values.string(getValue(columnName));
  }

  public Double getDouble(String columnName) {
    return Values.double_(getValue(columnName));
  }

  public byte getByte(String columnName) {
    return Values.tinyint(getValue(columnName));
  }

  public Boolean getBoolean(String columnName) {
    return Values.bool(getValue(columnName));
  }

  private Value getValue(String columnName) {
    int i = firstIndexOf(columnName);
    if (i < 0) {
      throw new IllegalArgumentException(String.format("Column '%s' does not exist", columnName));
    }
    return row.getValues(i);
  }

  /** @return the index, or <0 if the column does not exist. */
  private int firstIndexOf(String columnName) {
    for (int i = 0; i < columns.size(); i++) {
      ColumnSpec column = columns.get(i);
      if (column.getName().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }
}
