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
package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import io.stargate.auth.TypedKeyValue;
import io.stargate.db.query.BoundQuery;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Abstract the queries needed to process a mutation operation, and how the results will be turned
 * into a GraphQL response.
 */
class MutationPayload<ResultT> {

  private final List<BoundQuery> queries;
  private final List<List<TypedKeyValue>> primaryKeys;
  private final Function<List<MutationResult>, ResultT> resultBuilder;

  MutationPayload(
      List<BoundQuery> queries,
      List<List<TypedKeyValue>> primaryKeys,
      Function<List<MutationResult>, ResultT> resultBuilder) {
    this.queries = queries;
    this.primaryKeys = primaryKeys;
    this.resultBuilder = resultBuilder;
  }

  MutationPayload(
      BoundQuery query,
      List<TypedKeyValue> primaryKey,
      Function<List<MutationResult>, ResultT> resultBuilder) {
    this(Collections.singletonList(query), Collections.singletonList(primaryKey), resultBuilder);
  }

  /**
   * The queries to execute.
   *
   * <p>Note that this will generally be a singleton; bulk inserts are currently the only case where
   * there can be multiple queries.
   */
  List<BoundQuery> getQueries() {
    return queries;
  }

  /**
   * The primary key impacted by each query (in the same order as the queries).
   *
   * <p>Note that this could be computed from the queries, but fetcher implementations already need
   * to do it for authorization, so we pass them here to avoid recomputing them a second time in
   * {@link MutationFetcher}.
   */
  List<List<TypedKeyValue>> getPrimaryKeys() {
    return primaryKeys;
  }

  Function<List<MutationResult>, ResultT> getResultBuilder() {
    return resultBuilder;
  }
}
