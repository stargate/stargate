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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import com.google.common.collect.ImmutableList;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import java.util.List;
import java.util.function.Function;

public class DbColumnGetter {

  private static final List<Function<CqlTable, List<ColumnSpec>>> ALL_COLUMN_GETTERS =
      ImmutableList.of(
          CqlTable::getPartitionKeyColumnsList,
          CqlTable::getClusteringKeyColumnsList,
          CqlTable::getColumnsList,
          CqlTable::getStaticColumnsList);

  private final NameMapping nameMapping;

  public DbColumnGetter(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  public String getDBColumnName(CqlTable table, String fieldName) {
    ColumnSpec column = getColumn(table, fieldName);
    return (column == null) ? null : column.getName();
  }

  public ColumnSpec getColumn(CqlTable table, String fieldName) {
    String columnName = nameMapping.getCqlName(table, fieldName);
    if (columnName == null) {
      return null;
    }
    for (Function<CqlTable, List<ColumnSpec>> getter : ALL_COLUMN_GETTERS) {
      for (ColumnSpec column : getter.apply(table)) {
        if (columnName.equals(column.getName())) {
          return column;
        }
      }
    }
    return null;
  }
}
