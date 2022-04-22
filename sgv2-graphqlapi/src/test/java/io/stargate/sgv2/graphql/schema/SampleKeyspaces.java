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
package io.stargate.sgv2.graphql.schema;

import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.BOOLEAN;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.CUSTOM;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.FLOAT;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.INT;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.LINESTRING;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.POINT;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.POLYGON;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.TEXT;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.TIMEUUID;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.UNRECOGNIZED;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.UUID;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.VARCHAR;

import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Basic;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;

public class SampleKeyspaces {

  public static final CqlKeyspaceDescribe LIBRARY =
      keyspace("library")
          .addTables(
              table("books")
                  .addPartitionKeyColumns(column("title", VARCHAR))
                  .addColumns(column("author", VARCHAR)))
          .addTables(
              table("authors")
                  .addPartitionKeyColumns(column("author", VARCHAR))
                  .addClusteringKeyColumns(column("title", VARCHAR)))
          .build();

  private static final TypeSpec.Udt UDT_B = udt("B").putFields("i", basic(INT)).build();
  private static final TypeSpec.Udt UDT_A = udt("A").putFields("b", frozen(UDT_B)).build();

  public static final CqlKeyspaceDescribe UDTS =
      keyspace("udts")
          .addTypes(UDT_A)
          .addTypes(UDT_B)
          .addTables(table("TestTable").addPartitionKeyColumns(column("a", frozen(UDT_A))))
          .build();

  public static final CqlKeyspaceDescribe TUPLES =
      keyspace("tuples_ks")
          .addTables(
              table("tuples")
                  .addPartitionKeyColumns(column("id", INT))
                  .addColumns(column("value0", tuple(FLOAT, FLOAT)))
                  .addColumns(column("value1", tuple(UUID, VARCHAR, INT)))
                  .addColumns(column("value2", tuple(TIMEUUID, BOOLEAN))))
          .build();

  public static final CqlKeyspaceDescribe COLLECTIONS =
      keyspace("collections")
          .addTables(
              table("PkListTable").addPartitionKeyColumns(column("l", frozenList(basic(INT)))))
          .addTables(
              table("RegularListTable")
                  .addPartitionKeyColumns(column("k", INT))
                  .addColumns(column("l", list(basic(INT)))))
          .addTables(table("PkSetTable").addPartitionKeyColumns(column("s", frozenSet(basic(INT)))))
          .addTables(
              table("RegularSetTable")
                  .addPartitionKeyColumns(column("k", INT))
                  .addColumns(column("s", set(basic(INT)))))
          .addTables(
              table("PkMapTable")
                  .addPartitionKeyColumns(column("m", frozenMap(basic(INT), basic(VARCHAR)))))
          .addTables(
              table("RegularMapTable")
                  .addPartitionKeyColumns(column("k", INT))
                  .addColumns(column("m", map(basic(INT), basic(VARCHAR)))))
          .addTables(
              table("NestedCollections")
                  .addPartitionKeyColumns(column("k", INT))
                  .addColumns(column("c", map(basic(INT), list(set(basic(VARCHAR)))))))
          .build();

  public static final CqlKeyspaceDescribe IOT =
      keyspace("iot")
          .addTables(
              table("readings")
                  .addPartitionKeyColumns(column("id", INT))
                  .addClusteringKeyColumns(column("year", INT))
                  .addClusteringKeyColumns(column("month", INT))
                  .addClusteringKeyColumns(column("day", INT))
                  .addColumns(column("value", FLOAT)))
          .build();

  public static final CqlKeyspaceDescribe SCALARS =
      keyspace("scalars_ks").addTables(buildScalarsTable()).build();

  private static CqlTable.Builder buildScalarsTable() {
    CqlTable.Builder table = table("Scalars").addPartitionKeyColumns(column("id", INT));
    for (Basic basic : Basic.values()) {
      if (basic == UNRECOGNIZED
          || basic == POINT
          || basic == LINESTRING
          || basic == POLYGON
          || basic == CUSTOM
          || basic == TEXT) {
        continue;
      }
      String name = getScalarColumnName(basic);
      table.addColumns(column(name, basic));
    }
    return table;
  }

  public static String getScalarColumnName(Basic basic) {
    return basic.name().toLowerCase() + "value";
  }

  private static CqlKeyspaceDescribe.Builder keyspace(String name) {
    return CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(CqlKeyspace.newBuilder().setName(name));
  }

  private static CqlTable.Builder table(String name) {
    return CqlTable.newBuilder().setName(name);
  }

  private static ColumnSpec.Builder column(String name, TypeSpec type) {
    return ColumnSpec.newBuilder().setName(name).setType(type);
  }

  private static ColumnSpec.Builder column(String name, Basic basicType) {
    return column(name, basic(basicType));
  }

  private static TypeSpec basic(Basic basicType) {
    return TypeSpec.newBuilder().setBasic(basicType).build();
  }

  private static TypeSpec list(TypeSpec element) {
    return list(element, false);
  }

  private static TypeSpec frozenList(TypeSpec element) {
    return list(element, true);
  }

  private static TypeSpec list(TypeSpec element, boolean frozen) {
    return TypeSpec.newBuilder()
        .setList(TypeSpec.List.newBuilder().setElement(element).setFrozen(frozen))
        .build();
  }

  private static TypeSpec set(TypeSpec element) {
    return set(element, false);
  }

  private static TypeSpec frozenSet(TypeSpec element) {
    return set(element, true);
  }

  private static TypeSpec set(TypeSpec element, boolean frozen) {
    return TypeSpec.newBuilder()
        .setSet(TypeSpec.Set.newBuilder().setElement(element).setFrozen(frozen))
        .build();
  }

  private static TypeSpec map(TypeSpec key, TypeSpec value) {
    return map(key, value, false);
  }

  private static TypeSpec frozenMap(TypeSpec key, TypeSpec value) {
    return map(key, value, true);
  }

  private static TypeSpec map(TypeSpec key, TypeSpec value, boolean frozen) {
    return TypeSpec.newBuilder()
        .setMap(TypeSpec.Map.newBuilder().setKey(key).setValue(value).setFrozen(frozen))
        .build();
  }

  private static TypeSpec.Udt.Builder udt(String name) {
    return TypeSpec.Udt.newBuilder().setName(name);
  }

  private static TypeSpec tuple(Basic... elementTypes) {
    TypeSpec.Tuple.Builder tuple = TypeSpec.Tuple.newBuilder();
    for (Basic elementType : elementTypes) {
      tuple.addElements(basic(elementType));
    }
    return TypeSpec.newBuilder().setTuple(tuple).build();
  }

  private static TypeSpec frozen(TypeSpec.Udt udt) {
    return TypeSpec.newBuilder().setUdt(TypeSpec.Udt.newBuilder(udt).setFrozen(true)).build();
  }
}
