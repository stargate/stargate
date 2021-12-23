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

package io.stargate.web.docsapi.service.write.db;

import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.web.docsapi.service.JsonShreddedRow;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class InsertQueryBuilder {

  private final int maxDepth;
  private final List<ValueModifier> insertValueModifiers;

  /**
   * Constructs the query builder for inserting document rows based on the defined max depth.
   *
   * @param maxDepth Max depth of the document storage
   */
  public InsertQueryBuilder(int maxDepth) {
    this.maxDepth = maxDepth;
    insertValueModifiers =
        Arrays.stream(DocsApiConstants.ALL_COLUMNS_NAMES.apply(maxDepth))
            .map(ValueModifier::marker)
            .collect(Collectors.toList());
  }

  /**
   * Builds the query for inserting one row of a document data.
   *
   * @param queryBuilder Query builder
   * @param keyspace keyspace
   * @param table table
   * @return BuiltQuery
   */
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table) {
    return queryBuilder
        .get()
        .insertInto(keyspace, table)
        .value(insertValueModifiers)
        .timestamp()
        .build();
  }

  /**
   * Binds the query built with this query builder from supplied data.
   *
   * @param builtQuery Prepared query built by this query builder.
   * @param documentId The document id the row is inserted for.
   * @param row {@link JsonShreddedRow} containing data for the row.
   * @param timestamp Timestamp
   * @param numericBooleans If number booleans should be used
   * @param <E> generics param
   * @return Bound query.
   */
  public <E extends Query<? extends BoundQuery>> BoundQuery bind(
      E builtQuery,
      String documentId,
      JsonShreddedRow row,
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
    Object[] values = new Object[maxDepth + 6];
    // key at index 0
    values[0] = documentId;

    // then the path, based on the max depth
    List<String> path = row.getPath();
    for (int i = 0; i < maxDepth; i++) {
      if (i < path.size()) {
        values[i + 1] = path.get(i);
      } else {
        values[i + 1] = "";
      }
    }

    // rest at the end
    values[maxDepth + 1] = row.getLeaf();
    values[maxDepth + 2] = row.getStringValue();
    values[maxDepth + 3] = row.getDoubleValue();
    values[maxDepth + 4] = convertToBackendBooleanValue(row.getBooleanValue(), numericBooleans);

    // respect the timestamp
    values[maxDepth + 5] = timestamp;

    return builtQuery.bind(values);
  }

  private Object convertToBackendBooleanValue(Boolean value, boolean numericBooleans) {
    if (null == value) {
      return null;
    } else if (numericBooleans) {
      return Boolean.TRUE.equals(value) ? 1 : 0;
    }
    return value;
  }
}
