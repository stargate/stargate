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
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Shared base class for all document query builder. */
public abstract class AbstractDeleteQueryBuilder {

  /** @return Consumer for altering the where conditions. */
  protected abstract Consumer<QueryBuilder.QueryBuilder__40> whereConsumer();

  /**
   * Builds the query for this query builder.
   *
   * @param queryBuilder Query builder
   * @param keyspace keyspace
   * @param table table
   * @return BuiltQuery
   */
  public final BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table) {
    QueryBuilder.QueryBuilder__40 where =
        queryBuilder
            .get()
            .delete()
            .from(keyspace, table)
            .timestamp()
            .where(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ);

    whereConsumer().accept(where);
    return where.build();
  }

  /** @return Returns the order of binding for the document id and timestamp. */
  protected Object[] getBindValues(String documentId, long timestamp) {
    return new Object[] {timestamp, documentId};
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
  public final <E extends Query<? extends BoundQuery>> BoundQuery bind(
      E builtQuery, String documentId, long timestamp) {
    Object[] values = getBindValues(documentId, timestamp);
    return builtQuery.bind(values);
  }
}
