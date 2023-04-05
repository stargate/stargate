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

package io.stargate.web.docsapi.service.query.search.resolver.filter.impl;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.search.db.impl.DocumentSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link CandidatesFilter} that works with set of {@link FilterExpression}s that are on the same
 * path containing only persistence conditions.
 */
public class PersistenceCandidatesFilter implements CandidatesFilter {

  private final DocumentSearchQueryBuilder queryBuilder;

  private final ExecutionContext context;

  private final DocsApiConfiguration config;

  private PersistenceCandidatesFilter(
      Collection<FilterExpression> expressions,
      ExecutionContext context,
      DocsApiConfiguration config) {
    boolean hasInMemory =
        expressions.stream().anyMatch(e -> !e.getCondition().isPersistenceCondition());

    if (hasInMemory) {
      throw new IllegalArgumentException(
          "PersistenceCandidatesDocumentsResolver works only with the persistence conditions.");
    }

    this.queryBuilder = new DocumentSearchQueryBuilder(expressions, config);
    this.context = createContext(context, expressions);
    this.config = config;
  }

  public static Function<ExecutionContext, CandidatesFilter> forExpression(
      FilterExpression expression, DocsApiConfiguration config) {
    return forExpressions(Collections.singletonList(expression), config);
  }

  public static Function<ExecutionContext, CandidatesFilter> forExpressions(
      Collection<FilterExpression> expressions, DocsApiConfiguration config) {
    return context -> new PersistenceCandidatesFilter(expressions, context, config);
  }

  @Override
  public Single<? extends Query<? extends BoundQuery>> prepareQuery(
      DataStore dataStore, String keyspace, String collection) {
    return RxUtils.singleFromFuture(
            () -> {
              FilterPath filterPath = queryBuilder.getFilterPath();
              Integer limit = filterPath.isFixed() ? 1 : null;
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(
                      dataStore::queryBuilder,
                      keyspace,
                      collection,
                      limit,
                      DocsApiConstants.KEY_COLUMN_NAME,
                      DocsApiConstants.LEAF_COLUMN_NAME);
              return dataStore.prepare(query);
            })
        .cache();
  }

  @Override
  public Maybe<RawDocument> bindAndFilter(
      QueryExecutor queryExecutor,
      Query<? extends BoundQuery> preparedQuery,
      RawDocument document) {
    BoundQuery query = preparedQuery.bind(document.id());

    // execute query
    // page size 2 with limit 1 to ensure no additional pages fetched (only on fixed path)
    // use max storage page size otherwise as we have the doc id
    FilterPath filterPath = queryBuilder.getFilterPath();
    int pageSize = filterPath.isFixed() ? 2 : config.getMaxStoragePageSize();
    return queryExecutor.queryDocs(query, pageSize, false, null, context).take(1).singleElement();
  }

  private ExecutionContext createContext(
      ExecutionContext context, Collection<FilterExpression> expressions) {
    String expressionDesc =
        expressions.stream()
            .map(FilterExpression::getDescription)
            .collect(Collectors.joining(" AND "));

    return context.nested("FILTER: " + expressionDesc);
  }
}
