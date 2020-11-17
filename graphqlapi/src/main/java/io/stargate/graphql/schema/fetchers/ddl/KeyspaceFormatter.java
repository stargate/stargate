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
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

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
    formatChildren(
        builder,
        "table",
        keyspace::tables,
        keyspace::table,
        KeyspaceFormatter::buildTable,
        environment);
    formatChildren(
        builder,
        "type",
        keyspace::userDefinedTypes,
        keyspace::userDefinedType,
        KeyspaceFormatter::buildUdt,
        environment);

    builder.put("dcs", buildDcs(keyspace));
    return builder.build();
  }

  private static <ChildT> void formatChildren(
      ImmutableMap.Builder<String, Object> builder,
      String childFieldName,
      Supplier<Iterable<ChildT>> allChildrenGetter,
      Function<String, ChildT> childByNameGetter,
      Function<ChildT, Map<String, Object>> converter,
      DataFetchingEnvironment environment) {

    // All children query, for example `keyspace(name: "ks") { tables }`
    String allChildrenName = childFieldName + "s";
    if (environment.getSelectionSet().getField(allChildrenName) != null) {
      List<Map<String, Object>> formattedChildren = new ArrayList<>();
      for (ChildT child : allChildrenGetter.get()) {
        formattedChildren.add(converter.apply(child));
      }
      builder.put(allChildrenName, formattedChildren);
    }

    // Named child query, for example `keyspace(name: "ks") { table(name: "t") }`
    SelectedField childField = environment.getSelectionSet().getField(childFieldName);
    if (childField != null) {
      String name = (String) childField.getArguments().get("name");
      ChildT child = childByNameGetter.apply(name);
      if (child != null) {
        builder.put(childFieldName, converter.apply(child));
      }
    }
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

  private static Map<String, Object> buildTable(Table table) {
    return ImmutableMap.of(
        "name", table.name(),
        "columns", buildColumns(table.columns(), true));
  }

  private static Map<String, Object> buildUdt(UserDefinedType type) {
    return ImmutableMap.of(
        "name", type.name(),
        "fields", buildColumns(type.columns(), false));
  }

  private static List<Map<String, Object>> buildColumns(List<Column> columns, boolean includeKind) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Column column : columns) {
      list.add(buildColumn(column, includeKind));
    }
    return list;
  }

  private static Map<String, Object> buildColumn(Column column, boolean includeKind) {
    return includeKind
        ? ImmutableMap.of(
            "kind", buildColumnKind(column),
            "name", column.name(),
            "type", buildDataType(column.type()))
        : ImmutableMap.of(
            "name", column.name(),
            "type", buildDataType(column.type()));
  }

  private static Map<String, Object> buildDataType(Column.ColumnType columnType) {
    if (columnType.isUserDefined()) {
      return ImmutableMap.of(
          "basic",
          buildBasicType(columnType),
          "info",
          ImmutableMap.of("name", columnType.name(), "frozen", columnType.isFrozen()));
    } else if (columnType.isCollection() || columnType.isTuple()) {
      return ImmutableMap.of(
          "basic", buildBasicType(columnType),
          "info", buildParameterizedDataTypeInfo(columnType));
    } else {
      return ImmutableMap.of("basic", buildBasicType(columnType));
    }
  }

  private static Map<String, Object> buildParameterizedDataTypeInfo(Column.ColumnType columnType) {
    assert columnType.isParameterized();
    List<Map<String, Object>> list = new ArrayList<>();
    for (Column.ColumnType type : columnType.parameters()) {
      list.add(buildDataType(type));
    }
    return ImmutableMap.of("subTypes", list, "frozen", columnType.isFrozen());
  }

  private static String buildBasicType(Column.ColumnType columnType) {
    return columnType.rawType().name().toUpperCase();
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
