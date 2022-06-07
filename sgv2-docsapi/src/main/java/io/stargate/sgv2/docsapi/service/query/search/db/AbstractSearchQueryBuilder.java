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

package io.stargate.sgv2.docsapi.service.query.search.db;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.QueryBuilderImpl;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Abstract class that can create a query for a document search. */
public abstract class AbstractSearchQueryBuilder {

  /** Document properties that should be used to resolve column names. */
  protected final DocumentProperties documentProperties;

  public AbstractSearchQueryBuilder(DocumentProperties documentProperties) {
    this.documentProperties = documentProperties;
  }

  /** @return All fixed predicates. */
  protected abstract Collection<BuiltCondition> getPredicates();

  /** @return Predicates that depends on the binding value. */
  protected abstract Collection<BuiltCondition> getBindPredicates();

  /** @return Should <code>ALLOW FILTERING</code> be used. */
  protected abstract boolean allowFiltering();

  /**
   * Builds the query without limit (no functions).
   *
   * @param keyspace keyspace
   * @param table table
   * @param columns columns to query, must not be empty
   * @return Completable future that returns prepared query
   */
  public QueryOuterClass.Query buildQuery(String keyspace, String table, String... columns) {
    return buildQuery(keyspace, table, null, Collections.emptyList(), columns);
  }

  /**
   * Builds the query with limit (no functions).
   *
   * @param keyspace keyspace
   * @param table table
   * @param columns columns to query, must not be empty
   * @return Completable future that returns prepared query
   */
  public QueryOuterClass.Query buildQuery(
      String keyspace, String table, Integer limit, String... columns) {
    return buildQuery(keyspace, table, limit, Collections.emptyList(), columns);
  }

  public QueryOuterClass.Query buildQuery(
      String keyspace, String table, List<QueryBuilderImpl.FunctionCall> functions) {
    return buildQuery(keyspace, table, null, functions);
  }

  public QueryOuterClass.Query buildQuery(
      String keyspace,
      String table,
      Integer limit,
      List<QueryBuilderImpl.FunctionCall> functions,
      String... columns) {
    QueryBuilder.QueryBuilder__47 builder =
        new QueryBuilder()
            .select()
            .column(columns)
            .function(functions)
            .writeTimeColumn(documentProperties.tableProperties().leafColumnName())
            .from(keyspace, table)
            .where(getPredicates())
            .where(getBindPredicates())
            .limit(limit);

    // resolve allow limit
    if (allowFiltering()) {
      return builder.allowFiltering().build();
    } else {
      return builder.build();
    }
  }
}
