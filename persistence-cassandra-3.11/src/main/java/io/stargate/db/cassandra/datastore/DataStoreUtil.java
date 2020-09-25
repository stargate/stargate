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
package io.stargate.db.cassandra.datastore;

import static java.util.stream.Collectors.toList;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableMap;
import io.stargate.db.datastore.common.util.ColumnUtils;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableUserDefinedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.javatuples.Pair;

public class DataStoreUtil {
  private static final Map<Class<? extends AbstractType>, Column.Type> TYPE_MAPPINGS;

  static {
    Map<Class<? extends AbstractType>, Column.Type> types = new HashMap<>();
    Arrays.asList(Column.Type.values())
        .forEach(
            ct -> {
              if (ct != Column.Type.Tuple
                  && ct != Column.Type.List
                  && ct != Column.Type.Map
                  && ct != Column.Type.Set
                  && ct != Column.Type.UDT) {
                types.put(ColumnUtils.toInternalType(ct).getClass(), ct);
              }
            });
    TYPE_MAPPINGS = ImmutableMap.copyOf(types);
  }

  /**
   * When using {@link CqlCollection#entryEq} we're actually passing a {@link Pair} where the first
   * value is the map key and the second value is map value and we need to extract the second value.
   */
  public static Object maybeExtractParameterIfMapPair(Object parameter) {
    return parameter instanceof Pair ? ((Pair) parameter).getValue1() : parameter;
  }

  public static Column.ColumnType getTypeFromInternal(AbstractType abstractType) {
    if (abstractType instanceof ReversedType) {
      return getTypeFromInternal(((ReversedType) abstractType).baseType);
    }
    if (abstractType instanceof MapType) {
      return Column.Type.Map.of(
              getTypeFromInternal(((MapType) abstractType).getKeysType()),
              getTypeFromInternal(((MapType) abstractType).getValuesType()))
          .frozen(!abstractType.isMultiCell());
    } else if (abstractType instanceof SetType) {
      return Column.Type.Set.of(getTypeFromInternal(((SetType) abstractType).getElementsType()))
          .frozen(!abstractType.isMultiCell());
    } else if (abstractType instanceof ListType) {
      return Column.Type.List.of(getTypeFromInternal(((ListType) abstractType).getElementsType()))
          .frozen(!abstractType.isMultiCell());
    } else if (abstractType.getClass().equals(TupleType.class)) {
      TupleType tupleType = ((TupleType) abstractType);
      return Column.Type.Tuple.of(
              ((TupleType) abstractType)
                  .allTypes().stream()
                      .map(t -> getTypeFromInternal(t))
                      .toArray(Column.ColumnType[]::new))
          .frozen(!tupleType.isMultiCell());
    } else if (abstractType.getClass().equals(UserType.class)) {
      UserType udt = (UserType) abstractType;
      return ImmutableUserDefinedType.builder()
          .keyspace(udt.keyspace)
          .name(udt.getNameAsString())
          .addAllColumns(getUDTColumns(udt))
          .build()
          .frozen(!udt.isMultiCell());
    }

    Column.Type type = TYPE_MAPPINGS.get(abstractType.getClass());
    Preconditions.checkArgument(
        type != null, "Unknown type mapping for %s", abstractType.getClass());
    return type;
  }

  public static List<Column> getUDTColumns(org.apache.cassandra.db.marshal.UserType userType) {
    List<Column> columns = new ArrayList<>(userType.fieldTypes().size());
    for (int i = 0; i < userType.fieldTypes().size(); i++) {
      columns.add(
          ImmutableColumn.builder()
              .name(userType.fieldName(i).toString())
              .type(getTypeFromInternal(userType.fieldType(i)))
              .kind(Column.Kind.Regular)
              .build());
    }
    return columns;
  }

  public static List<Column> getUDTColumns(UserDefinedType userType) {
    return userType.getFieldNames().stream()
        .map(
            name -> {
              DataType type = userType.getFieldTypes().get(userType.firstIndexOf(name));
              return ImmutableColumn.builder()
                  .name(name.asInternal())
                  .type(getTypeFromDriver(type))
                  .kind(Column.Kind.Regular)
                  .build();
            })
        .collect(toList());
  }

  public static Column.ColumnType getTypeFromDriver(DataType type) {
    if (type.getProtocolCode() == ProtocolConstants.DataType.MAP) {
      com.datastax.oss.driver.api.core.type.MapType mapType =
          (com.datastax.oss.driver.api.core.type.MapType) type;
      return Column.Type.Map.of(
              getTypeFromDriver(mapType.getKeyType()), getTypeFromDriver(mapType.getValueType()))
          .frozen(mapType.isFrozen());
    } else if (type.getProtocolCode() == ProtocolConstants.DataType.SET) {
      com.datastax.oss.driver.api.core.type.SetType setType =
          (com.datastax.oss.driver.api.core.type.SetType) type;
      return Column.Type.Set.of(getTypeFromDriver(setType.getElementType()))
          .frozen(setType.isFrozen());
    } else if (type.getProtocolCode() == ProtocolConstants.DataType.LIST) {
      com.datastax.oss.driver.api.core.type.ListType listType =
          (com.datastax.oss.driver.api.core.type.ListType) type;
      return Column.Type.List.of(getTypeFromDriver(listType.getElementType()))
          .frozen(listType.isFrozen());
    } else if (type.getProtocolCode() == ProtocolConstants.DataType.TUPLE) {
      com.datastax.oss.driver.api.core.type.TupleType tupleType =
          (com.datastax.oss.driver.api.core.type.TupleType) type;
      return Column.Type.Tuple.of(
          (tupleType)
              .getComponentTypes().stream()
                  .map(DataStoreUtil::getTypeFromDriver)
                  .toArray(Column.ColumnType[]::new));
    } else if (type.getProtocolCode() == ProtocolConstants.DataType.CUSTOM) {
      // Each custom type is merely identified by the fully qualified class name that represents
      // this type
      // server-side
      String customClassName = type.toString().replace("'", "");
      throw new IllegalStateException(String.format("Unknown CUSTOM type %s", customClassName));
    } else if (type.getProtocolCode() == ProtocolConstants.DataType.UDT) {
      com.datastax.oss.driver.api.core.type.UserDefinedType udt =
          (com.datastax.oss.driver.api.core.type.UserDefinedType) type;
      return ImmutableUserDefinedType.builder()
          .keyspace(udt.getKeyspace().asInternal())
          .name(udt.getName().asInternal())
          .isFrozen(udt.isFrozen())
          .addAllColumns(getUDTColumns(udt))
          .build();
    }

    return Column.Type.fromCqlDefinitionOf(type.toString()); // PrimitiveType
  }
}
