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

import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.sgv2.common.cql.CqlStrings;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.Column.Kind;
import io.stargate.sgv2.common.cql.builder.Column.Order;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for fetchers that execute a single DDL query, such as a CREATE KEYSPACE or DROP TABLE.
 */
public abstract class DdlQueryFetcher extends CassandraFetcher<Boolean> {

  @Override
  protected Boolean get(DataFetchingEnvironment environment, StargateGraphqlContext context) {
    context.getBridge().executeQuery(buildQuery(environment, context));
    return true;
  }

  protected abstract Query buildQuery(
      DataFetchingEnvironment environment, StargateGraphqlContext context);

  protected String decodeType(Object typeObject) {
    @SuppressWarnings("unchecked")
    Map<String, Object> type = (Map<String, Object>) typeObject;
    String basic = (String) type.get("basic");
    @SuppressWarnings("unchecked")
    Map<String, Object> info = (Map<String, Object>) type.get("info");
    String name = info == null ? null : (String) info.get("name");
    List<?> subTypes = info == null ? null : (List<?>) info.get("subTypes");
    boolean frozen = info != null && info.containsKey("frozen") && (Boolean) info.get("frozen");

    String elementType;
    switch (basic) {
      case "INT":
      case "INET":
      case "TIMEUUID":
      case "TIMESTAMP":
      case "BIGINT":
      case "TIME":
      case "DURATION":
      case "VARINT":
      case "UUID":
      case "BOOLEAN":
      case "TINYINT":
      case "SMALLINT":
      case "ASCII":
      case "DECIMAL":
      case "BLOB":
      case "VARCHAR":
      case "TEXT":
      case "DOUBLE":
      case "COUNTER":
      case "DATE":
      case "FLOAT":
        return basic.toLowerCase();
      case "LIST":
        if (info == null) {
          throw new IllegalArgumentException(
              "List type should contain an 'info' field specifying the sub type");
        }
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("List sub types should contain 1 item");
        }
        elementType = decodeType(subTypes.get(0));
        return String.format(frozen ? "frozen<list<%s>>" : "list<%s>", elementType);
      case "SET":
        if (info == null) {
          throw new IllegalArgumentException(
              "Set type should contain an 'info' field specifying the sub type");
        }
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("Set sub types should contain 1 item");
        }
        elementType = decodeType(subTypes.get(0));
        return String.format(frozen ? "frozen<set<%s>>" : "set<%s>", elementType);
      case "MAP":
        if (info == null) {
          throw new IllegalArgumentException(
              "Map type should contain an 'info' field specifying the sub types");
        }
        if (subTypes == null || subTypes.size() != 2) {
          throw new IllegalArgumentException("Map sub types should contain 2 items");
        }
        String keyType = decodeType(subTypes.get(0));
        String valueType = decodeType(subTypes.get(1));
        return String.format(frozen ? "frozen<map<%s, %s>>" : "map<%s, %s>", keyType, valueType);
      case "UDT":
        if (name == null) {
          throw new IllegalArgumentException(
              "UDT type should contain an 'info' field specifying the UDT name");
        }
        String udtName = CqlStrings.doubleQuote(name);
        return frozen ? String.format("frozen<%s>", udtName) : udtName;
      case "TUPLE":
        if (info == null) {
          throw new IllegalArgumentException(
              "TUPLE type should contain an 'info' field specifying the sub types");
        }
        if (subTypes.isEmpty()) {
          throw new IllegalArgumentException("TUPLE type should have at least one sub type");
        }
        return subTypes.stream()
            .map(this::decodeType)
            .collect(Collectors.joining(", ", "frozen<tuple<", ">>"));
    }
    throw new RuntimeException(String.format("Data type %s is not supported", basic));
  }

  protected Column decodeColumn(Map<String, Object> key, Kind kind) {
    return ImmutableColumn.builder()
        .name((String) key.get("name"))
        .kind(kind)
        .type(decodeType(key.get("type")))
        .order(kind == Kind.CLUSTERING ? decodeClusteringOrder((String) key.get("order")) : null)
        .build();
  }

  private Order decodeClusteringOrder(String order) {
    if (order == null) {
      // Use the same default as CQL
      return Order.ASC;
    }
    return Order.valueOf(order.toUpperCase());
  }

  protected List<Column> decodeColumns(List<Map<String, Object>> columnList, Kind kind) {
    if (columnList == null) {
      return Collections.emptyList();
    }
    List<Column> columns = new ArrayList<>(columnList.size());
    for (Map<String, Object> entry : columnList) {
      columns.add(decodeColumn(entry, kind));
    }
    return columns;
  }
}
