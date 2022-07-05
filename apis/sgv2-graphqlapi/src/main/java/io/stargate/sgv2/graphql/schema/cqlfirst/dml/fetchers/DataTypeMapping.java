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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Tuple;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.bridge.proto.QueryOuterClass.UdtValue;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Provides the logic for adapting values from graphql to DB and vice versa. */
class DataTypeMapping {

  /**
   * Converts a value coming from the GraphQL runtime into a value that can be sent to the bridge.
   */
  static Value toGrpcValue(TypeSpec type, Object graphQLValue, NameMapping nameMapping) {
    if (graphQLValue == null) {
      return Values.NULL;
    }
    switch (type.getSpecCase()) {
      case LIST:
        return convertCollection(type.getList().getElement(), graphQLValue, nameMapping);
      case SET:
        return convertCollection(type.getSet().getElement(), graphQLValue, nameMapping);
      case MAP:
        return convertMap(
            type.getMap().getKey(), type.getMap().getValue(), graphQLValue, nameMapping);
      case UDT:
        return convertUdt(type.getUdt(), graphQLValue, nameMapping);
      case TUPLE:
        return convertTuple(type.getTuple(), graphQLValue, nameMapping);
      case BASIC:
        // GraphQL has already coerced the value to the expected Java type, so we can cast directly.
        switch (type.getBasic()) {
          case ASCII:
          case VARCHAR:
            return Values.of(((String) graphQLValue));
          case INET:
            return Values.of((InetAddress) graphQLValue);
          case BIGINT:
          case COUNTER:
            return Values.of((Long) graphQLValue);
          case BLOB:
            return Values.of((ByteBuffer) graphQLValue);
          case BOOLEAN:
            return Values.of((Boolean) graphQLValue);
          case DECIMAL:
            return Values.of((BigDecimal) graphQLValue);
          case DOUBLE:
            return Values.of((Double) graphQLValue);
          case FLOAT:
            return Values.of((Float) graphQLValue);
          case INT:
            return Values.of((Integer) graphQLValue);
          case TIMESTAMP:
            return Values.of(((Instant) graphQLValue).toEpochMilli());
          case UUID:
          case TIMEUUID:
            return Values.of(((UUID) graphQLValue));
          case VARINT:
            return Values.of((BigInteger) graphQLValue);
          case DATE:
            return Values.of((LocalDate) graphQLValue);
          case TIME:
            return Values.of((LocalTime) graphQLValue);
          case SMALLINT:
            return Values.of((Short) graphQLValue);
          case TINYINT:
            return Values.of((Byte) graphQLValue);
          case DURATION:
            return Values.of((CqlDuration) graphQLValue);
          default:
            throw new AssertionError("Unhandled basic type " + type.getBasic());
        }
      default:
        throw new AssertionError("Unhandled type " + type.getSpecCase());
    }
  }

  private static Value convertCollection(
      TypeSpec elementType, Object graphQLvalue, NameMapping nameMapping) {
    Collection<?> graphQLCollection = (Collection<?>) graphQLvalue;
    Value[] dbCollection = new Value[graphQLCollection.size()];
    int i = 0;
    for (Object element : graphQLCollection) {
      dbCollection[i] = toGrpcValue(elementType, element, nameMapping);
      i++;
    }
    return Values.of(dbCollection);
  }

  private static Value convertMap(
      TypeSpec keyType, TypeSpec valueType, Object graphQLValue, NameMapping nameMapping) {
    Map<Value, Value> dbMap = new LinkedHashMap<>(); // Preserving input order is nice-to-have
    @SuppressWarnings("unchecked")
    Collection<Map<String, Object>> graphQLMap = (Collection<Map<String, Object>>) graphQLValue;
    for (Map<String, Object> entry : graphQLMap) {
      Value key = toGrpcValue(keyType, entry.get("key"), nameMapping);
      Value value = toGrpcValue(valueType, entry.get("value"), nameMapping);
      dbMap.put(key, value);
    }
    return Values.of(dbMap);
  }

  @SuppressWarnings("unchecked")
  private static Value convertUdt(Udt udtType, Object graphQLValue, NameMapping nameMapping) {
    Map<String, Object> object = (Map<String, Object>) graphQLValue;
    UdtValue.Builder udtValue = UdtValue.newBuilder();
    for (Map.Entry<String, Object> entry : object.entrySet()) {
      String fieldName = nameMapping.getCqlName(udtType, entry.getKey());
      TypeSpec fieldType = udtType.getFieldsMap().get(fieldName);
      Value value = toGrpcValue(fieldType, entry.getValue(), nameMapping);
      udtValue.putFields(fieldName, value);
    }
    return Value.newBuilder().setUdt(udtValue).build();
  }

  @SuppressWarnings("unchecked")
  private static Value convertTuple(Tuple type, Object graphQLValue, NameMapping nameMapping) {
    Map<String, Object> object = (Map<String, Object>) graphQLValue;
    Value[] dbCollection = new Value[object.size()];
    int i = 0;
    for (TypeSpec subType : type.getElementsList()) {
      Object item = object.get("item" + i);
      dbCollection[i] = toGrpcValue(subType, item, nameMapping);
      i += 1;
    }
    return Values.of(dbCollection);
  }

  /**
   * Converts result Row into a map suitable to serve it via GraphQL.
   *
   * @param columnSpecs the description of the columns in the row (from ResultSet.columns).
   */
  static Map<String, Object> toGraphqlValue(
      NameMapping nameMapping, CqlTable table, List<ColumnSpec> columnSpecs, Row row) {
    Map<String, Object> map = new HashMap<>(columnSpecs.size());
    int i = 0;
    for (ColumnSpec column : columnSpecs) {
      Value value = row.getValues(i);
      String graphqlName = nameMapping.getGraphqlName(table, column);
      if (graphqlName != null && !value.hasNull()) {
        map.put(graphqlName, toGraphqlValue(nameMapping, column.getType(), value));
      }
      i += 1;
    }
    return map;
  }

  private static Object toGraphqlValue(NameMapping nameMapping, TypeSpec type, Value dbValue) {
    if (dbValue.hasNull()) {
      return null;
    }
    switch (type.getSpecCase()) {
      case LIST:
      case SET:
        TypeSpec elementType =
            type.hasList() ? type.getList().getElement() : type.getSet().getElement();
        return dbValue.getCollection().getElementsList().stream()
            .map(e -> toGraphqlValue(nameMapping, elementType, e))
            .collect(Collectors.toList());
      case MAP:
        TypeSpec keyType = type.getMap().getKey();
        TypeSpec valueType = type.getMap().getValue();
        List<Value> mapElements = dbValue.getCollection().getElementsList();
        assert mapElements.size() % 2 == 0;
        List<Map<String, Object>> graphqlEntries = new ArrayList<>(mapElements.size() / 2);
        for (int i = 0; i < mapElements.size(); i += 2) {
          Object graphqlKey = toGraphqlValue(nameMapping, keyType, mapElements.get(i));
          Object graphqlValue = toGraphqlValue(nameMapping, valueType, mapElements.get(i + 1));
          graphqlEntries.add(ImmutableMap.of("key", graphqlKey, "value", graphqlValue));
        }
        return graphqlEntries;
      case UDT:
        Udt udtType = type.getUdt();
        UdtValue udtValue = dbValue.getUdt();
        Map<String, Object> graphqlUdt =
            Maps.newLinkedHashMapWithExpectedSize(udtValue.getFieldsCount());
        for (Map.Entry<String, Value> entry : udtValue.getFieldsMap().entrySet()) {
          String cqlName = entry.getKey();
          String graphqlName = nameMapping.getGraphqlName(udtType, cqlName);
          graphqlUdt.put(
              graphqlName,
              toGraphqlValue(nameMapping, udtType.getFieldsMap().get(cqlName), entry.getValue()));
        }
        return graphqlUdt;
      case TUPLE:
        List<TypeSpec> fieldTypes = type.getTuple().getElementsList();
        List<Value> dbFields = dbValue.getCollection().getElementsList();
        Map<String, Object> graphqlTuple = Maps.newLinkedHashMapWithExpectedSize(dbFields.size());
        for (int i = 0; i < dbFields.size(); i++) {
          TypeSpec fieldType = fieldTypes.get(i);
          graphqlTuple.put("item" + i, toGraphqlValue(nameMapping, fieldType, dbFields.get(i)));
        }
        return graphqlTuple;
      case BASIC:
        switch (type.getBasic()) {
          case ASCII:
          case VARCHAR:
            return Values.string(dbValue);
          case INET:
            return Values.inet(dbValue);
          case BIGINT:
          case COUNTER:
            return Values.bigint(dbValue);
          case BLOB:
            return Values.byteBuffer(dbValue);
          case BOOLEAN:
            return Values.bool(dbValue);
          case DECIMAL:
            return Values.decimal(dbValue);
          case DOUBLE:
            return Values.double_(dbValue);
          case FLOAT:
            return Values.float_(dbValue);
          case INT:
            return Values.int_(dbValue);
          case TIMESTAMP:
            return Instant.ofEpochMilli(Values.bigint(dbValue));
          case UUID:
          case TIMEUUID:
            return Values.uuid(dbValue);
          case VARINT:
            return Values.varint(dbValue);
          case DATE:
            return Values.date(dbValue);
          case TIME:
            return Values.time(dbValue);
          case SMALLINT:
            return Values.smallint(dbValue);
          case TINYINT:
            return Values.tinyint(dbValue);
          case DURATION:
            return Values.duration(dbValue);
          default:
            throw new AssertionError("Unhandled basic type " + type.getBasic());
        }
      default:
        throw new AssertionError("Unhandled type " + type.getSpecCase());
    }
  }
}
