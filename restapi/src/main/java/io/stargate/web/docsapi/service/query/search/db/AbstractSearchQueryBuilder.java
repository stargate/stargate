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

package io.stargate.web.docsapi.service.query.search.db;

import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.web.docsapi.service.query.QueryConstants;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/** Abstract class that can create a query for a document search. */
public abstract class AbstractSearchQueryBuilder {

  /** @return All fixed predicates. */
  protected abstract Collection<BuiltCondition> getPredicates();

  /** @return Predicates that depends on the binding value. */
  protected abstract Map<String, Predicate> getBindPredicates();

  /** @return Should <code>ALLOW FILTERING</code> be used. */
  protected abstract boolean allowFiltering();

  /**
   * Builds the query without limit.
   *
   * @param queryBuilder Method for query builder.
   * @param keyspace keyspace
   * @param table table
   * @param columns columns to query, must now be empty
   * @return Completable future that returns prepared query
   */
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder, String keyspace, String table, String... columns) {
    return buildQuery(queryBuilder, keyspace, table, null, columns);
  }

  /**
   * Builds the query with limit.
   *
   * @param queryBuilder Method for query builder.
   * @param keyspace keyspace
   * @param table table
   * @param limit limit
   * @param columns columns to query, must now be empty
   * @return Completable future that returns prepared query
   */
  public BuiltQuery<? extends BoundQuery> buildQuery(
      Supplier<QueryBuilder> queryBuilder,
      String keyspace,
      String table,
      Integer limit,
      String... columns) {
    QueryBuilder.QueryBuilder__21 builder =
        queryBuilder
            .get()
            .select()
            .column(columns)
            .writeTimeColumn(QueryConstants.LEAF_COLUMN_NAME) // TODO needed?
            .from(keyspace, table)
            .where(getPredicates());

    // then all bind able predicates
    for (Map.Entry<String, Predicate> entry : getBindPredicates().entrySet()) {
      builder = builder.where(entry.getKey(), entry.getValue());
    }

    // resolve the allow limit
    QueryBuilder.QueryBuilder__44 limitBuilder = builder.limit(limit);
    if (allowFiltering()) {
      return limitBuilder.allowFiltering().build();
    } else {
      return limitBuilder.build();
    }
  }
}
