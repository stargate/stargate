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
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.function.Supplier;

public class DeleteQueryBuilder {

  /** Constructs the query builder for deleting all document rows for a single document. */
  public DeleteQueryBuilder() {}

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
    return queryBuilder
        .get()
        .delete()
        .from(keyspace, table)
        .timestamp()
        .where(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ)
        .build();
  }

  /**
   * Binds the query built with this query builder from supplied data.
   *
   * @param builtQuery Prepared query built by this query builder.
   * @param documentId The document id the row is inserted for.
   * @param timestamp Timestamp
   * @param <E> generics param
   * @return Bound query.
   */
  public <E extends Query<? extends BoundQuery>> BoundQuery bind(
      E builtQuery, String documentId, long timestamp) {
    return builtQuery.bind(timestamp, documentId);
  }
}
