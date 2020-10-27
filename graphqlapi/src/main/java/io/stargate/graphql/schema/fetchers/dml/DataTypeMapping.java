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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.NameMapping;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Provides the logic for adapting values from graphql to DB and vice versa. */
class DataTypeMapping {

  /**
   * Converts a value coming from the GraphQL runtime into a term that can be used with {@link
   * QueryBuilder}.
   */
  static Term toCqlTerm(Column.ColumnType type, Object value, NameMapping nameMapping) {
    StringBuilder raw = new StringBuilder();
    format(type, value, nameMapping, raw);
    return QueryBuilder.raw(raw.toString());
  }

  private static void format(
      Column.ColumnType type, Object value, NameMapping nameMapping, StringBuilder out) {
    if (type.isCollection()) {
      if (type.rawType() == Column.Type.List) {
        formatElements(type.parameters().get(0), (Collection<?>) value, '[', ']', nameMapping, out);
      } else if (type.rawType() == Column.Type.Set) {
        formatElements(type.parameters().get(0), (Collection<?>) value, '{', '}', nameMapping, out);
      } else if (type.rawType() == Column.Type.Map) {
        formatMap(type, value, nameMapping, out);
      } else {
        throw new AssertionError("Invalid collection type " + type);
      }
    } else if (type.isUserDefined()) {
      formatUdt((UserDefinedType) type, value, nameMapping, out);
    } else if (type.isTuple()) {
      formatTuple(type, value, nameMapping, out);
    } else { // primitive
      @SuppressWarnings("unchecked")
      TypeCodec<Object> codec = type.codec();
      out.append(codec.format(value));
    }
  }

  private static void formatElements(
      Column.ColumnType elementType,
      Collection<?> elements,
      char startChar,
      char endChar,
      NameMapping nameMapping,
      StringBuilder out) {
    out.append(startChar);
    boolean first = true;
    for (Object element : elements) {
      if (first) {
        first = false;
      } else {
        out.append(',');
      }
      format(elementType, element, nameMapping, out);
    }
    out.append(endChar);
  }

  private static void formatMap(
      Column.ColumnType type, Object value, NameMapping nameMapping, StringBuilder out) {
    out.append('{');
    @SuppressWarnings("unchecked")
    Collection<Map<String, Object>> entries = (Collection<Map<String, Object>>) value;
    Column.ColumnType keyType = type.parameters().get(0);
    Column.ColumnType valueType = type.parameters().get(1);
    boolean first = true;
    for (Map<String, Object> entry : entries) {
      if (first) {
        first = false;
      } else {
        out.append(',');
      }
      format(keyType, entry.get("key"), nameMapping, out);
      out.append(':');
      format(valueType, entry.get("value"), nameMapping, out);
    }
    out.append('}');
  }

  private static void formatUdt(
      UserDefinedType type, Object value, NameMapping nameMapping, StringBuilder out) {
    type = type.frozen(false);
    out.append('{');
    @SuppressWarnings("unchecked")
    Map<String, Object> object = (Map<String, Object>) value;
    boolean first = true;
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      if (first) {
        first = false;
      } else {
        out.append(',');
      }
      String fieldName = nameMapping.getCqlName(type, entry.getKey());
      Column.ColumnType fieldType = type.fieldType(fieldName);
      out.append('"').append(fieldName).append("\":");
      format(fieldType, entry.getValue(), nameMapping, out);
    }
    out.append('}');
  }

  private static void formatTuple(
      Column.ColumnType type, Object value, NameMapping nameMapping, StringBuilder out) {
    out.append('(');
    @SuppressWarnings("unchecked")
    Map<String, Object> mapValue = (Map<String, Object>) value;
    List<Column.ColumnType> subTypes = type.parameters();

    // Track null values.
    // Note that first item can't be null as enforced by the schema.
    boolean hasANullItem = false;

    for (int i = 0; i < subTypes.size(); i++) {
      Object item = mapValue.get("item" + i);

      if (i > 0) {
        if (item == null) {
          hasANullItem = true;
          continue;
        }

        if (hasANullItem) {
          throw new UnsupportedOperationException(
              "Tuple can have a null item followed by a non-null item");
        }
        out.append(',');
      }

      format(subTypes.get(i), item, nameMapping, out);
    }

    out.append(')');
  }

  /** Converts result Row into a map suitable to serve it via GraphQL. */
  static Map<String, Object> toGraphQLValue(NameMapping nameMapping, Table table, Row row) {
    List<Column> columns = row.columns();
    Map<String, Object> map = new HashMap<>(columns.size());
    for (Column column : columns) {
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null && !row.isNull(column.name())) {
        map.put(graphqlName, toGraphQLValue(nameMapping, column, row));
      }
    }
    return map;
  }

  private static Object toGraphQLValue(NameMapping nameMapping, Column column, Row row) {
    Object dbValue = row.getObject(column.name());
    return toGraphQLValue(nameMapping, column.type(), dbValue);
  }

  private static Object toGraphQLValue(
      NameMapping nameMapping, Column.ColumnType type, Object dbValue) {
    if (dbValue == null) {
      return null;
    } else if (type.isCollection()) {
      if (type.rawType() == Column.Type.List || type.rawType() == Column.Type.Set) {
        Collection<?> dbCollection = (Collection<?>) dbValue;
        return dbCollection.stream()
            .map(item -> toGraphQLValue(nameMapping, type.parameters().get(0), item))
            .collect(Collectors.toList());
      } else if (type.rawType() == Column.Type.Map) {
        Map<?, ?> dbMap = (Map<?, ?>) dbValue;
        Column.ColumnType keyType = type.parameters().get(0);
        Column.ColumnType valueType = type.parameters().get(1);
        return dbMap.entrySet().stream()
            .map(
                e -> {
                  Map<String, Object> m = new HashMap<>(2);
                  m.put("key", toGraphQLValue(nameMapping, keyType, e.getKey()));
                  m.put("value", toGraphQLValue(nameMapping, valueType, e.getValue()));
                  return m;
                })
            .collect(Collectors.toList());
      } else {
        throw new AssertionError("Invalid collection type " + type);
      }
    } else if (type.isUserDefined()) {
      UserDefinedType udt = (UserDefinedType) type.frozen(false);
      UdtValue udtValue = (UdtValue) dbValue;
      Map<String, Object> result = new HashMap<>(udt.columns().size());
      for (Column column : udt.columns()) {
        Object dbFieldValue = udtValue.getObject(CqlIdentifier.fromInternal(column.name()));
        String graphQlFieldName = nameMapping.getGraphqlName(udt, column);
        if (dbFieldValue != null && graphQlFieldName != null) {
          Object graphQlFieldValue = toGraphQLValue(nameMapping, column.type(), dbFieldValue);
          result.put(graphQlFieldName, graphQlFieldValue);
        }
      }
      return result;
    } else if (type.isTuple()) {
      TupleValue tuple = (TupleValue) dbValue;
      Map<String, Object> result = new HashMap<>(tuple.size());
      for (int i = 0; i < tuple.size(); i++) {
        result.put(
            "item" + i, toGraphQLValue(nameMapping, type.parameters().get(i), tuple.getObject(i)));
      }
      return result;
    } else { // primitive
      return dbValue;
    }
  }
}
