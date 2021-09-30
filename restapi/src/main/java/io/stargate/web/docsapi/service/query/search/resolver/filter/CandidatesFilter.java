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

package io.stargate.web.docsapi.service.query.search.resolver.filter;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;

/** Interface for candidates filtering. */
public interface CandidatesFilter {

  /**
   * Returns single that emmit prepared query that this filter needs.
   *
   * @param dataStore {@link DataStore}
   * @param keyspace Keyspace
   * @param collection Collection
   * @return Prepared query in a single
   */
  @NonNull
  Single<? extends Query<? extends BoundQuery>> prepareQuery(
      DataStore dataStore, String keyspace, String collection);

  /**
   * Executes a filter for given {@link RawDocument} oprating with the query that was supplied in
   * the {@link #prepareQuery(DataStore, String, String)}.
   *
   * <p>Returns the Maybe. If this maybe emits an item, we consider this filter to be successful. If
   * the maybe does not emit an item, we consider that filter is not passed.
   *
   * @param queryExecutor {@link QueryExecutor}
   * @param preparedQuery Query provided as part of the {@link #prepareQuery(DataStore, String,
   *     String)}
   * @param document Document to filter
   * @return Maybe, if emits the item, then filter is considered as passed
   */
  Maybe<?> bindAndFilter(
      QueryExecutor queryExecutor, Query<? extends BoundQuery> preparedQuery, RawDocument document);
}
