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
package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.Schema.CqlKeyspace;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyspaceDto {

  // Parses the "replication" keyspace option, for example:
  //   { 'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2 }
  private static final Splitter.MapSplitter REPLICATION_SPLITTER =
      Splitter.on(',')
          .trimResults(CharMatcher.anyOf("{} "))
          .withKeyValueSeparator(Splitter.on(':').trimResults(CharMatcher.anyOf("' ")));

  private final CqlKeyspace keyspace;
  private final List<TypeSpec.Udt> udts;
  private final StargateBridgeClient bridge;

  public KeyspaceDto(CqlKeyspaceDescribe keyspaceDescribe, StargateBridgeClient bridge) {
    this.keyspace = keyspaceDescribe.getCqlKeyspace();
    this.udts = keyspaceDescribe.getTypesList();
    // Don't store the tables, it's simpler to re-query the bridge since it handles authorizations
    this.bridge = bridge;
  }

  public String getName() {
    return keyspace.getName();
  }

  public List<Map<String, String>> getDcs() {
    String replication = keyspace.getOptionsMap().get("replication");
    if (replication == null) {
      return Collections.emptyList();
    }
    List<Map<String, String>> dcs = new ArrayList<>();
    for (Map.Entry<String, String> entries : REPLICATION_SPLITTER.split(replication).entrySet()) {
      if (entries.getKey().equals("class")) continue;
      if (entries.getKey().equals("replication_factor")) continue;
      dcs.add(
          ImmutableMap.of(
              "name", entries.getKey(),
              "replicas", entries.getValue()));
    }
    return dcs;
  }

  public List<Map<String, Object>> getTables() {
    return bridge.getTables(keyspace.getName()).stream()
        .map(KeyspaceDto::buildTable)
        .collect(Collectors.toList());
  }

  public Map<String, Object> getTable(DataFetchingEnvironment environment) {
    String name = environment.getArgument("name");
    return bridge
        .getTable(keyspace.getName(), name, true)
        .map(KeyspaceDto::buildTable)
        .orElse(null);
  }

  public List<Map<String, Object>> getTypes() {
    return udts.stream().map(KeyspaceDto::buildUdt).collect(Collectors.toList());
  }

  public Map<String, Object> getType(DataFetchingEnvironment environment) {
    String name = environment.getArgument("name");
    return udts.stream()
        .filter(u -> name.equals(u.getName()))
        .findFirst()
        .map(KeyspaceDto::buildUdt)
        .orElse(null);
  }

  private static Map<String, Object> buildTable(CqlTable table) {
    List<Map<String, Object>> columns = new ArrayList<>();
    buildColumns(table.getPartitionKeyColumnsList(), "PARTITION", columns);
    buildColumns(table.getClusteringKeyColumnsList(), "CLUSTERING", columns);
    buildColumns(table.getColumnsList(), "REGULAR", columns);
    buildColumns(table.getStaticColumnsList(), "STATIC", columns);
    return ImmutableMap.of("name", table.getName(), "columns", columns);
  }

  private static void buildColumns(
      List<ColumnSpec> columns, String kind, List<Map<String, Object>> acc) {
    columns.forEach(c -> acc.add(buildColumn(c, kind)));
  }

  private static Map<String, Object> buildColumn(ColumnSpec column, String kind) {
    return ImmutableMap.of(
        "kind", kind,
        "name", column.getName(),
        "type", buildDataType(column.getType()));
  }

  private static Map<String, Object> buildUdt(TypeSpec.Udt type) {
    return ImmutableMap.of(
        "name", type.getName(),
        "fields", buildFields(type.getFieldsMap()));
  }

  private static List<Map<String, Object>> buildFields(Map<String, TypeSpec> fields) {
    return fields.entrySet().stream()
        .map(e -> ImmutableMap.of("name", e.getKey(), "type", buildDataType(e.getValue())))
        .collect(Collectors.toList());
  }

  private static Map<String, Object> buildDataType(TypeSpec typeSpec) {
    switch (typeSpec.getSpecCase()) {
      case UDT:
        TypeSpec.Udt udt = typeSpec.getUdt();
        return ImmutableMap.of(
            "basic",
            "UDT",
            "info",
            ImmutableMap.of("name", udt.getName(), "frozen", udt.getFrozen()));
      case LIST:
        TypeSpec.List list = typeSpec.getList();
        return buildParameterizedType("LIST", list.getFrozen(), list.getElement());
      case SET:
        TypeSpec.Set set = typeSpec.getSet();
        return buildParameterizedType("SET", set.getFrozen(), set.getElement());
      case MAP:
        TypeSpec.Map map = typeSpec.getMap();
        return buildParameterizedType("MAP", map.getFrozen(), map.getKey(), map.getValue());
      case TUPLE:
        TypeSpec.Tuple tuple = typeSpec.getTuple();
        return buildParameterizedType(
            "TUPLE", true, tuple.getElementsList().toArray(new TypeSpec[0]));
      case BASIC:
        TypeSpec.Basic grpc = typeSpec.getBasic();
        String graphql = (grpc == TypeSpec.Basic.VARCHAR) ? "TEXT" : grpc.name().toUpperCase();
        return ImmutableMap.of("basic", graphql);
      default:
        throw new IllegalArgumentException("Unsupported type " + typeSpec);
    }
  }

  private static Map<String, Object> buildParameterizedType(
      String basic, boolean frozen, TypeSpec... subTypes) {
    return ImmutableMap.of(
        "basic",
        basic,
        "info",
        ImmutableMap.of(
            "subTypes",
            Arrays.stream(subTypes).map(KeyspaceDto::buildDataType).collect(Collectors.toList()),
            "frozen",
            frozen));
  }
}
