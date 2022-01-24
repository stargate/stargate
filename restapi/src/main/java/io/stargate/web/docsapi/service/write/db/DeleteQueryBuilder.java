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
import io.stargate.db.query.Predicate;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class DeleteQueryBuilder {

  private final int maxDepth;

  /**
   * Constructs the query builder for deleting all document rows for a single document.
   *
   * @param maxDepth
   */
  public DeleteQueryBuilder(int maxDepth) {
    this.maxDepth = maxDepth;
  }

  /**
   * Builds the query for deleting all rows for a document.
   *
   * @param queryBuilder Query builder
   * @param keyspace keyspace
   * @param table table
   * @return BuiltQuery
   */
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table) {
    return buildQuery(queryBuilder, keyspace, table, 0);
  }

  /**
   * Builds the query for deleting rows for a document on a specific sub-path.
   *
   * <p><i>Note that binding of this query still requires the sub-path values.</i>
   *
   * @param queryBuilder Query builder
   * @param keyspace keyspace
   * @param table table
   * @param subDocumentPath The sub-path of the document to delete. If empty, deletes a complete
   *     document.
   * @return BuiltQuery
   */
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder,
      String keyspace,
      String table,
      List<String> subDocumentPath) {
    int size = Optional.ofNullable(subDocumentPath).map(List::size).orElse(0);
    return buildQuery(queryBuilder, keyspace, table, size);
  }

  /**
   * Builds the query for deleting rows for a document on a specific sub-path size.
   *
   * @param queryBuilder Query builder
   * @param keyspace keyspace
   * @param table table
   * @param subDocumentPathSize The sub-path size.
   * @return BuiltQuery
   */
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table, int subDocumentPathSize) {
    // make sure we are not exceeding the max depth
    if (subDocumentPathSize > maxDepth) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }

    QueryBuilder.QueryBuilder__40 where =
        queryBuilder
            .get()
            .delete()
            .from(keyspace, table)
            .timestamp()
            .where(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ);

    // if we have the sub-document path add that as well
    for (int i = 0; i < subDocumentPathSize; i++) {
      where.where(DocsApiConstants.P_COLUMN_NAME.apply(i), Predicate.EQ);
    }

    return where.build();
  }

  /**
   * Binds the query built with this query builder from supplied data.
   *
   * @param builtQuery Prepared query built by this query builder.
   * @param documentId The document id the row is inserted for.
   * @param subDocumentPath Sub-document path to bind.
   * @param timestamp Timestamp
   * @param <E> generics param
   * @return Bound query.
   */
  public <E extends Query<? extends BoundQuery>> BoundQuery bind(
      E builtQuery, String documentId, List<String> subDocumentPath, long timestamp) {
    int subPathSize = Optional.ofNullable(subDocumentPath).map(List::size).orElse(0);
    Object[] values = new Object[subPathSize + 2];

    // timestamp and document id as first
    values[0] = timestamp;
    values[1] = documentId;

    // then sub-document paths
    for (int i = 0; i < subPathSize; i++) {
      values[i + 2] = subDocumentPath.get(i);
    }

    return builtQuery.bind(values);
  }
}
