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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import io.stargate.proto.QueryOuterClass.Query;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Abstract the queries needed to process a mutation operation, and how the results will be turned
 * into a GraphQL response.
 */
class MutationPayload<ResultT> {

  private final List<Query> queries;
  private final List<List<TypedKeyValue>> primaryKeys;
  private final Function<List<MutationResult>, ResultT> resultBuilder;

  MutationPayload(
      List<Query> queries,
      List<List<TypedKeyValue>> primaryKeys,
      Function<List<MutationResult>, ResultT> resultBuilder) {
    this.queries = queries;
    this.primaryKeys = primaryKeys;
    this.resultBuilder = resultBuilder;
  }

  MutationPayload(
      Query query,
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
  List<Query> getQueries() {
    return queries;
  }

  /**
   * The primary key impacted by each query (in the same order as the queries).
   *
   * <p>Note that this is only needed if the payload will be executed as part of a conditional
   * batch. So for queries that target more than one row (and therefore can't be used in a
   * conditional batch), it can be left empty.
   */
  List<List<TypedKeyValue>> getPrimaryKeys() {
    return primaryKeys;
  }

  Function<List<MutationResult>, ResultT> getResultBuilder() {
    return resultBuilder;
  }
}
