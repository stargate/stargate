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

package io.stargate.sgv2.docsapi.service.query.search.resolver.filter;

import io.reactivex.rxjava3.annotations.NonNull;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;

/** Interface for candidates filtering. */
public interface CandidatesFilter {

  /**
   * Returns single that emmit query that this filter needs.
   *
   * @param keyspace Keyspace
   * @param collection Collection
   * @return Prepared query in a single
   */
  @NonNull
  Uni<QueryOuterClass.Query> prepareQuery(String keyspace, String collection); // TODO rename

  /**
   * Executes a filter for given {@link RawDocument} operating with the query that was supplied in
   * the {@link #prepareQuery(String, String)}.
   *
   * <p>Returns the Uni that must never emit <code>null</code>. If this uni emits true, we consider
   * this filter to be successful. If the uni emits false, we consider that filter is not passed.
   *
   * @param preparedQuery Query provided as part of the {@link #prepareQuery(String, String)}
   * @param document Document to filter
   * @return Uni, if emits true, then filter is considered as passed
   */
  Uni<Boolean> bindAndFilter(
      QueryExecutor queryExecutor, QueryOuterClass.Query preparedQuery, RawDocument document);
}
