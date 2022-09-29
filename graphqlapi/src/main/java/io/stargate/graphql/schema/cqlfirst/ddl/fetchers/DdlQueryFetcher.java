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
package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class for fetchers that execute a single DDL query, such as a CREATE KEYSPACE or DROP TABLE.
 */
public abstract class DdlQueryFetcher extends CassandraFetcher<Boolean> {

  @Override
  protected Boolean get(DataFetchingEnvironment environment, StargateGraphqlContext context)
      throws Exception {
    context
        .getDataStore()
        .execute(buildQuery(environment, context.getDataStore().queryBuilder(), context).bind())
        .get();
    return true;
  }

  protected abstract Query<?> buildQuery(
      DataFetchingEnvironment environment, QueryBuilder builder, StargateGraphqlContext context)
      throws UnauthorizedException;

  protected ColumnType decodeType(Object typeObject) {
    @SuppressWarnings("unchecked")
    Map<String, Object> type = (Map<String, Object>) typeObject;
    String basic = (String) type.get("basic");
    @SuppressWarnings("unchecked")
    Map<String, Object> info = (Map<String, Object>) type.get("info");
    String name = info == null ? null : (String) info.get("name");
    List<?> subTypes = info == null ? null : (List<?>) info.get("subTypes");
    boolean frozen = info != null && info.containsKey("frozen") && (Boolean) info.get("frozen");

    switch (basic) {
      case "INT":
        return Type.Int;
      case "INET":
        return Type.Inet;
      case "TIMEUUID":
        return Type.Timeuuid;
      case "TIMESTAMP":
        return Type.Timestamp;
      case "BIGINT":
        return Type.Bigint;
      case "TIME":
        return Type.Time;
      case "DURATION":
        return Type.Duration;
      case "VARINT":
        return Type.Varint;
      case "UUID":
        return Type.Uuid;
      case "BOOLEAN":
        return Type.Boolean;
      case "TINYINT":
        return Type.Tinyint;
      case "SMALLINT":
        return Type.Smallint;
      case "ASCII":
        return Type.Ascii;
      case "DECIMAL":
        return Type.Decimal;
      case "BLOB":
        return Type.Blob;
      case "VARCHAR":
      case "TEXT":
        return Type.Text;
      case "DOUBLE":
        return Type.Double;
      case "COUNTER":
        return Type.Counter;
      case "DATE":
        return Type.Date;
      case "FLOAT":
        return Type.Float;
      case "LIST":
        if (info == null) {
          throw new IllegalArgumentException(
              "List type should contain an 'info' field specifying the sub type");
        }
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("List sub types should contain 1 item");
        }
        return Type.List.of(decodeType(subTypes.get(0))).frozen(frozen);
      case "SET":
        if (info == null) {
          throw new IllegalArgumentException(
              "Set type should contain an 'info' field specifying the sub type");
        }
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("Set sub types should contain 1 item");
        }
        subTypes = (List<?>) info.get("subTypes");
        return Type.Set.of(decodeType(subTypes.get(0))).frozen(frozen);
      case "MAP":
        if (info == null) {
          throw new IllegalArgumentException(
              "Map type should contain an 'info' field specifying the sub types");
        }
        if (subTypes == null || subTypes.size() != 2) {
          throw new IllegalArgumentException("Map sub types should contain 2 items");
        }
        return Type.Map.of(decodeType(subTypes.get(0)), decodeType(subTypes.get(1))).frozen(frozen);
      case "UDT":
        if (name == null) {
          throw new IllegalArgumentException(
              "UDT type should contain an 'info' field specifying the UDT name");
        }
        return UserDefinedType.reference(name).frozen(frozen);
      case "TUPLE":
        if (info == null) {
          throw new IllegalArgumentException(
              "TUPLE type should contain an 'info' field specifying the sub types");
        }
        if (subTypes.isEmpty()) {
          throw new IllegalArgumentException("TUPLE type should have at least one sub type");
        }
        ColumnType[] decodedSubTypes = new ColumnType[subTypes.size()];
        for (int i = 0; i < subTypes.size(); i++) {
          decodedSubTypes[i] = decodeType(subTypes.get(i));
        }
        return Type.Tuple.of(decodedSubTypes);
    }
    throw new RuntimeException(String.format("Data type %s is not supported", basic));
  }

  protected Column decodeColumn(Map<String, Object> key, Column.Kind kind) {
    return Column.create(
        (String) key.get("name"),
        kind,
        decodeType(key.get("type")),
        kind == Kind.Clustering ? decodeClusteringOrder((String) key.get("order")) : null);
  }

  private Order decodeClusteringOrder(String order) {
    if (order == null) {
      // Use the same default as CQL
      return Order.ASC;
    }
    return Order.valueOf(order.toUpperCase());
  }

  protected List<Column> decodeColumns(List<Map<String, Object>> columnList, Column.Kind kind) {
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
