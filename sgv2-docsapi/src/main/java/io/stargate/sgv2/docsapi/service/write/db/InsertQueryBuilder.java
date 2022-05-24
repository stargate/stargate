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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.write.db;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.ValueModifier;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import java.util.List;
import java.util.stream.Collectors;

public class InsertQueryBuilder {

  private final int maxDepth;
  private final List<ValueModifier> insertValueModifiers;

  public InsertQueryBuilder(DocumentProperties documentProperties) {
    maxDepth = documentProperties.maxDepth();
    insertValueModifiers =
        documentProperties.tableColumns().allColumns().stream()
            .map(c -> ValueModifier.marker(c.name()))
            .collect(Collectors.toList());
  }

  public Query buildAndBind(
      String keyspace,
      String table,
      Integer ttl,
      String documentId,
      JsonShreddedRow row,
      long timestamp,
      boolean numericBooleans) {
    return bind(buildQuery(keyspace, table, ttl), documentId, row, ttl, timestamp, numericBooleans);
  }

  /** Builds the query for inserting one row of a document data. */
  public Query buildQuery(String keyspace, String table, Integer ttl) {
    if (ttl != null) {
      return new QueryBuilder()
          .insertInto(keyspace, table)
          .value(insertValueModifiers)
          .ttl()
          .timestamp()
          .build();
    } else {
      return new QueryBuilder()
          .insertInto(keyspace, table)
          .value(insertValueModifiers)
          .timestamp()
          .build();
    }
  }

  /**
   * Binds the query built with this query builder from supplied data.
   *
   * @param builtQuery Prepared query built by this query builder.
   * @param documentId The document id the row is inserted for.
   * @param row {@link JsonShreddedRow} containing data for the row.
   * @param timestamp Timestamp
   * @param numericBooleans If number booleans should be used
   * @return Bound query.
   */
  public Query bind(
      Query builtQuery,
      String documentId,
      JsonShreddedRow row,
      Integer ttl,
      long timestamp,
      boolean numericBooleans) {
    // sanity check
    if (maxDepth != row.getMaxDepth()) {
      String msg =
          String.format(
              "Row with max depth of %d, cannot be bound with the insert query builder created for max depth of %d",
              row.getMaxDepth(), this.maxDepth);
      throw new IllegalArgumentException(msg);
    }

    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values = QueryOuterClass.Values.newBuilder();
    // key at index 0
    values.addValues(Values.of(documentId));

    // then the path, based on the max depth
    List<String> path = row.getPath();
    for (int i = 0; i < maxDepth; i++) {
      if (i < path.size()) {
        values.addValues(Values.of(path.get(i)));
      } else {
        values.addValues(Values.of(""));
      }
    }

    // rest at the end
    values.addValues(Values.of(row.getLeaf()));
    values.addValues(row.getStringValue() == null ? Values.NULL : Values.of(row.getStringValue()));
    values.addValues(row.getDoubleValue() == null ? Values.NULL : Values.of(row.getDoubleValue()));
    values.addValues(convertToBackendBooleanValue(row.getBooleanValue(), numericBooleans));

    if (ttl != null) {
      values.addValues(Values.of(ttl));
    }
    // respect the timestamp
    values.addValues(Values.of(timestamp));

    return Query.newBuilder(builtQuery).setValues(values).build();
  }

  private Value convertToBackendBooleanValue(Boolean value, boolean numericBooleans) {
    if (value == null) {
      return Values.NULL;
    } else if (numericBooleans) {
      return value ? Constants.NUMERIC_BOOLEAN_TRUE : Constants.NUMERIC_BOOLEAN_FALSE;
    }
    return Values.of(value);
  }
}
