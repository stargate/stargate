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
package io.stargate.graphql.schema.fetchers.ddl;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Helper class to format keyspace metadata into a GraphQL result object. */
class KeyspaceFormatter {

  static List<Map<String, Object>> formatResult(
      Set<Keyspace> keyspaces, DataFetchingEnvironment environment) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Keyspace keyspace : keyspaces) {
      list.add(formatResult(keyspace, environment));
    }
    return list;
  }

  static Map<String, Object> formatResult(Keyspace keyspace, DataFetchingEnvironment environment) {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("name", keyspace.name());
    if (environment.getSelectionSet().getField("tables") != null) {
      builder.put("tables", buildTables(keyspace.tables()));
    }

    SelectedField tableField;
    if ((tableField = environment.getSelectionSet().getField("table")) != null) {
      String tableName = (String) tableField.getArguments().get("name");
      Table table = keyspace.table(tableName);
      if (table != null) {
        builder.put("table", buildTable(table));
      }
    }
    builder.put("dcs", buildDcs(keyspace));
    return builder.build();
  }

  private static List<Map<String, String>> buildDcs(Keyspace keyspace) {
    List<Map<String, String>> list = new ArrayList<>();
    for (Map.Entry<String, String> entries : keyspace.replication().entrySet()) {
      if (entries.getKey().equals("class")) continue;
      if (entries.getKey().equals("replication_factor")) continue;
      list.add(
          ImmutableMap.of(
              "name", entries.getKey(),
              "replicas", entries.getValue()));
    }

    return list;
  }

  private static List<Map<String, Object>> buildTables(Set<Table> tables) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Table table : tables) {
      list.add(buildTable(table));
    }
    return list;
  }

  private static Map<String, Object> buildTable(Table table) {
    return ImmutableMap.of(
        "name", table.name(),
        "columns", buildColumns(table.columns()));
  }

  private static List<Map<String, Object>> buildColumns(List<Column> columns) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Column column : columns) {
      list.add(buildColumn(column));
    }
    return list;
  }

  private static Map<String, Object> buildColumn(Column column) {
    return ImmutableMap.of(
        "kind", buildColumnKind(column),
        "name", column.name(),
        "type", buildDataType(column.type()));
  }

  private static Map<String, Object> buildDataType(Column.ColumnType columntype) {
    if (columntype.isParameterized()) {
      return ImmutableMap.of(
          "basic", buildBasicType(columntype),
          "info", buildDataTypeInfo(columntype));
    }

    return ImmutableMap.of("basic", buildBasicType(columntype));
  }

  private static Map<String, List<Map<String, Object>>> buildDataTypeInfo(
      Column.ColumnType columntype) {
    assert columntype.isParameterized();
    List<Map<String, Object>> list = new ArrayList<>();
    for (Column.ColumnType type : columntype.parameters()) {
      list.add(buildDataType(type));
    }
    return ImmutableMap.of("subTypes", list);
  }

  private static String buildBasicType(Column.ColumnType columntype) {
    return columntype.rawType().name().toUpperCase();
  }

  private static String buildColumnKind(Column column) {
    switch (column.kind()) {
      case PartitionKey:
        return "PARTITION";
      case Clustering:
        return "CLUSTERING";
      case Regular:
        return "REGULAR";
      case Static:
        return "STATIC";
    }
    return "UNKNOWN";
  }
}
