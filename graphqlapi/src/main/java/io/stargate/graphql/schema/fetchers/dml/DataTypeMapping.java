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
package io.stargate.graphql.schema.fetchers.dml;

import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Provides the logic for adapting values from graphql to DB and vice versa. */
class DataTypeMapping {
  @SuppressWarnings("unchecked")
  static Object toDbValue(Column.ColumnType type, Object value) {
    if (value == null || !type.isCollection()) {
      return value;
    }

    if (type.rawType() == Column.Type.List) {
      if (!type.parameters().get(0).isCollection()) {
        return value;
      }

      return itemsToDbValue(type.parameters().get(0), (Collection<?>) value);
    }

    if (type.rawType() == Column.Type.Set) {
      return new HashSet<>(itemsToDbValue(type.parameters().get(0), (Collection<?>) value));
    }

    if (type.rawType() == Column.Type.Map) {
      Collection<Map<String, Object>> sourceMaps = (Collection<Map<String, Object>>) value;
      Map<Object, Object> targetMap = new HashMap<>(sourceMaps.size());
      Column.ColumnType keyType = type.parameters().get(0);
      Column.ColumnType valueType = type.parameters().get(1);
      for (Map<String, Object> kv : sourceMaps) {
        targetMap.put(toDbValue(keyType, kv.get("key")), toDbValue(valueType, kv.get("value")));
      }
      return targetMap;
    }

    return value;
  }

  private static List<?> itemsToDbValue(Column.ColumnType itemType, Collection<?> collection) {
    return collection.stream().map(item -> toDbValue(itemType, item)).collect(Collectors.toList());
  }

  /** Converts result Row into a map suitable to serve it via GraphQL. */
  static Map<String, Object> toGraphQLValue(NameMapping nameMapping, Table table, Row row) {
    List<Column> columns = row.columns();
    Map<String, Object> map = new HashMap<>(columns.size());
    for (Column column : columns) {
      if (!row.isNull(column.name())) {
        map.put(nameMapping.getColumnName(table).get(column), toGraphQLValue(column, row));
      }
    }
    return map;
  }

  private static Object toGraphQLValue(Column column, Row row) {
    Object dbValue = row.getObject(column.name());
    return toGraphQLValue(column.type(), dbValue);
  }

  private static Object toGraphQLValue(Column.ColumnType type, Object dbValue) {
    if (!type.isCollection() || dbValue == null) {
      return dbValue;
    }

    if (type.rawType() == Column.Type.Map) {
      // Convert from map to list of key/value maps
      Map<?, ?> dbMap = (Map<?, ?>) dbValue;
      Column.ColumnType keyType = type.parameters().get(0);
      Column.ColumnType valueType = type.parameters().get(1);
      return dbMap.entrySet().stream()
          .map(
              e -> {
                Map<String, Object> m = new HashMap<>(2);
                m.put("key", toGraphQLValue(keyType, e.getKey()));
                m.put("value", toGraphQLValue(valueType, e.getValue()));
                return m;
              })
          .collect(Collectors.toList());
    }

    // Nested list/set
    Column.ColumnType itemType = type.parameters().get(0);
    if (itemType.isCollection()) {
      Collection<?> dbCollection = (Collection<?>) dbValue;
      return dbCollection.stream()
          .map(item -> toGraphQLValue(itemType, item))
          .collect(Collectors.toList());
    }

    return dbValue;
  }
}
