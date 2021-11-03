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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.ParameterizedType.TupleType;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/** Provides the logic for adapting values from graphql to DB and vice versa. */
class DataTypeMapping {

  /** Converts a value coming from the GraphQL runtime into a DB value. */
  static Object toDBValue(ColumnType type, Object graphQLValue, NameMapping nameMapping) {
    if (graphQLValue == null) {
      return null;
    }
    if (type.isCollection()) {
      if (type.rawType() == Column.Type.List) {
        return convertCollection(type, graphQLValue, nameMapping, ArrayList::new);
      } else if (type.rawType() == Column.Type.Set) {
        return convertCollection(
            type, graphQLValue, nameMapping, Sets::newLinkedHashSetWithExpectedSize);
      } else if (type.rawType() == Column.Type.Map) {
        return convertMap(type, graphQLValue, nameMapping);
      } else {
        throw new AssertionError("Invalid collection type " + type);
      }
    } else if (type.isUserDefined()) {
      return convertUdt((UserDefinedType) type, graphQLValue, nameMapping);
    } else if (type.isTuple()) {
      return convertTuple((TupleType) type, graphQLValue, nameMapping);
    } else { // primitive
      return graphQLValue;
    }
  }

  private static Collection<Object> convertCollection(
      ColumnType type,
      Object graphQLvalue,
      NameMapping nameMapping,
      IntFunction<Collection<Object>> ctor) {
    ColumnType elementType = type.parameters().get(0);
    Collection<?> graphQLCollection = (Collection<?>) graphQLvalue;
    Collection<Object> collection = ctor.apply(graphQLCollection.size());
    for (Object element : graphQLCollection) {
      collection.add(toDBValue(elementType, element, nameMapping));
    }
    return collection;
  }

  private static Map<Object, Object> convertMap(
      ColumnType type, Object graphQLValue, NameMapping nameMapping) {
    Map<Object, Object> map = new LinkedHashMap<>(); // Preserving input order is nice-to-have
    @SuppressWarnings("unchecked")
    Collection<Map<String, Object>> graphQLMap = (Collection<Map<String, Object>>) graphQLValue;
    ColumnType keyType = type.parameters().get(0);
    ColumnType valueType = type.parameters().get(1);
    for (Map<String, Object> entry : graphQLMap) {
      Object key = toDBValue(keyType, entry.get("key"), nameMapping);
      Object value = toDBValue(valueType, entry.get("value"), nameMapping);
      map.put(key, value);
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private static UdtValue convertUdt(
      UserDefinedType type, Object graphQLValue, NameMapping nameMapping) {
    Map<String, Object> object = (Map<String, Object>) graphQLValue;
    UdtValue udt = type.create();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      String fieldName = nameMapping.getCqlName(type, entry.getKey());
      ColumnType fieldType = type.fieldType(fieldName);
      Object value = toDBValue(fieldType, entry.getValue(), nameMapping);
      udt = udt.set(fieldName, value, fieldType.codec());
    }
    return udt;
  }

  @SuppressWarnings("unchecked")
  private static TupleValue convertTuple(
      TupleType type, Object graphQLValue, NameMapping nameMapping) {
    Map<String, Object> object = (Map<String, Object>) graphQLValue;
    List<ColumnType> subTypes = type.parameters();
    TupleValue tuple = type.create();
    for (int i = 0; i < subTypes.size(); i++) {
      ColumnType subType = subTypes.get(i);
      Object item = object.get("item" + i);
      Object value = toDBValue(subType, item, nameMapping);
      tuple = tuple.set(i, value, subType.codec());
    }
    return tuple;
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

  private static Object toGraphQLValue(NameMapping nameMapping, ColumnType type, Object dbValue) {
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
        ColumnType keyType = type.parameters().get(0);
        ColumnType valueType = type.parameters().get(1);
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
