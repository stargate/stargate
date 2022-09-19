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
package io.stargate.it.cql.protocolV4;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

public class TypeSample<JavaTypeT> {

  private static final AtomicInteger COLUMN_COUNTER = new AtomicInteger();

  final DataType cqlType;
  final GenericType<JavaTypeT> javaType;
  final JavaTypeT value;
  final CqlIdentifier columnName;

  private TypeSample(
      DataType cqlType,
      GenericType<JavaTypeT> javaType,
      JavaTypeT value,
      CqlIdentifier columnName) {
    this.cqlType = cqlType;
    this.javaType = javaType;
    this.value = value;
    this.columnName = columnName;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TypeSample.class.getSimpleName() + "[", "]")
        .add("columnName=" + columnName)
        .add("cqlType=" + cqlType)
        .add("javaType=" + javaType)
        .toString();
  }

  public static <JavaTypeT> TypeSample<JavaTypeT> typeSample(
      DataType cqlType,
      GenericType<JavaTypeT> javaType,
      JavaTypeT value,
      CqlIdentifier columnName) {
    return new TypeSample<>(cqlType, javaType, value, columnName);
  }

  public static <JavaTypeT> TypeSample<JavaTypeT> typeSample(
      DataType cqlType, GenericType<JavaTypeT> javaType, JavaTypeT value) {
    return typeSample(cqlType, javaType, value, newColumnName());
  }

  public static <JavaTypeT> TypeSample<List<JavaTypeT>> listOf(TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.listOf(type.cqlType),
        GenericType.listOf(type.javaType),
        Arrays.asList(type.value));
  }

  public static <JavaTypeT> TypeSample<Set<JavaTypeT>> setOf(TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.setOf(type.cqlType),
        GenericType.setOf(type.javaType),
        ImmutableSet.of(type.value));
  }

  public static <JavaTypeT> TypeSample<Map<Integer, JavaTypeT>> mapOfIntTo(
      TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.mapOf(DataTypes.INT, type.cqlType),
        GenericType.mapOf(GenericType.INTEGER, type.javaType),
        ImmutableMap.of(1, type.value));
  }

  public static <JavaTypeT> TypeSample<Map<JavaTypeT, Integer>> mapToIntFrom(
      TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.mapOf(type.cqlType, DataTypes.INT),
        GenericType.mapOf(type.javaType, GenericType.INTEGER),
        ImmutableMap.of(type.value, 1));
  }

  public static <JavaTypeT> TypeSample<TupleValue> tupleOfIntAnd(TypeSample<JavaTypeT> type) {
    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, type.cqlType);
    return typeSample(tupleType, GenericType.TUPLE_VALUE, tupleType.newValue(1, type.value));
  }

  public static <JavaTypeT> TypeSample<UdtValue> udtOfIntAnd(
      TypeSample<JavaTypeT> type, CqlIdentifier keyspaceId) {
    CqlIdentifier columnName = TypeSample.newColumnName();
    UserDefinedType udtType =
        new UserDefinedTypeBuilder(keyspaceId, CqlIdentifier.fromInternal(columnName + "Type"))
            .withField("v1", DataTypes.INT)
            .withField("v2", type.cqlType)
            .build();
    UdtValue udtValue = udtType.newValue(1, type.value);
    return typeSample(udtType, GenericType.UDT_VALUE, udtValue, columnName);
  }

  public static CqlIdentifier newColumnName() {
    return CqlIdentifier.fromCql("column" + COLUMN_COUNTER.getAndIncrement());
  }
}
