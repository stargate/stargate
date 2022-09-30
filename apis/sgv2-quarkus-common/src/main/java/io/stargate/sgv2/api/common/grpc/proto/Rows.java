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
package io.stargate.sgv2.api.common.grpc.proto;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public class Rows {

  public static int getInt(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::int_);
  }

  public static long getBigint(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::bigint);
  }

  public static short getSmallint(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::smallint);
  }

  public static byte getTinyint(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::tinyint);
  }

  public static float getFloat(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::float_);
  }

  public static double getDouble(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::double_);
  }

  public static BigDecimal getDecimal(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::decimal);
  }

  public static BigInteger getVarint(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::varint);
  }

  public static UUID getUuid(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::uuid);
  }

  public static String getString(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::string);
  }

  public static Boolean getBoolean(Row row, String name, List<ColumnSpec> columns) {
    return getBasic(row, name, columns, Values::bool);
  }

  /** @return the index, or a negative value if the column does not exist. */
  public static int firstIndexOf(String name, List<ColumnSpec> columns) {
    for (int i = 0; i < columns.size(); i++) {
      ColumnSpec column = columns.get(i);
      if (column.getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  private static <V> V getBasic(
      Row row, String name, List<ColumnSpec> columns, Function<Value, V> getter) {
    Value value = getValue(row, name, columns);
    return getter.apply(value);
  }

  public static Value getValue(Row row, String name, List<ColumnSpec> columns) {
    int i = firstIndexOf(name, columns);
    if (i < 0) {
      throw new IllegalArgumentException(String.format("Column '%s' does not exist", name));
    }
    return row.getValues(i);
  }

  private Rows() {
    // intentionally empty
  }
}
