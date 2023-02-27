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

package io.stargate.web.docsapi.service.query.search.resolver.impl;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Predicate;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterPathSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * * {@link DocumentsResolver} that works with set of {@link FilterExpression}s that are on the same
 * * path containing only in-memory conditions.
 */
public class InMemoryDocumentsResolver implements DocumentsResolver {

  private final Collection<FilterExpression> expressions;

  private final AbstractSearchQueryBuilder queryBuilder;

  private final ExecutionContext context;

  private final boolean evaluateOnMissing;

  private final DocsApiConfiguration config;

  public InMemoryDocumentsResolver(
      FilterExpression expression, ExecutionContext context, DocsApiConfiguration config) {
    this(Collections.singletonList(expression), context, config);
  }

  public InMemoryDocumentsResolver(
      Collection<FilterExpression> expressions,
      ExecutionContext context,
      DocsApiConfiguration config) {
    boolean hasPersistence =
        expressions.stream().anyMatch(e -> e.getCondition().isPersistenceCondition());

    if (hasPersistence) {
      throw new IllegalArgumentException(
          "InMemoryCandidatesDocumentsResolver works only with the non persistence conditions.");
    }

    // if we have a single one that evaluate son the
    evaluateOnMissing =
        expressions.stream().anyMatch(e -> e.getCondition().isEvaluateOnMissingFields());

    this.expressions = expressions;
    this.queryBuilder =
        evaluateOnMissing
            ? new FullSearchQueryBuilder()
            : new FilterExpressionSearchQueryBuilder(expressions, config);
    this.context = createContext(context, expressions);
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {

    // if we have a filter path query then need all the columns on the filter, plus one additional
    // to match
    // otherwise we need all columns
    Integer neededDepth =
        Optional.of(queryBuilder)
            .filter(FilterPathSearchQueryBuilder.class::isInstance)
            .map(FilterPathSearchQueryBuilder.class::cast)
            .map(qb -> qb.getFilterPath().getPath().size() + 1)
            .orElse(config.getMaxDepth());

    String[] neededColumns = DocsApiConstants.ALL_COLUMNS_NAMES.apply(neededDepth);

    // prepare the query
    return RxUtils.singleFromFuture(
            () -> {
              DataStore dataStore = queryExecutor.getDataStore();
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(
                      dataStore::queryBuilder, keyspace, collection, neededColumns);
              return dataStore.prepare(query);
            })

        // cache it
        .cache()
        .flatMapPublisher(
            prepared -> {
              // once ready bind (no values) and fire
              BoundQuery query = prepared.bind();

              // in case we have a full search then base the page size on the app. doc size
              // otherwise start with the requested page size, plus one more than needed to stop
              // pre-fetching
              int pageSize =
                  evaluateOnMissing
                      ? config.getApproximateStoragePageSize(paginator.docPageSize)
                      : paginator.docPageSize + 1;
              return queryExecutor.queryDocs(
                  query, pageSize, true, paginator.getCurrentDbPageState(), context);
            })

        // then filter to match the expression (in-memory filters have no predicates on the values)
        .filter(matchAll(expressions));
  }

  private Predicate<? super RawDocument> matchAll(Collection<FilterExpression> expressions) {
    return rawDocument -> {
      for (FilterExpression expression : expressions) {
        if (!expression.test(rawDocument)) {
          return false;
        }
      }
      return true;
    };
  }

  private ExecutionContext createContext(
      ExecutionContext context, Collection<FilterExpression> expressions) {
    String expressionDesc =
        expressions.stream()
            .map(FilterExpression::getDescription)
            .collect(Collectors.joining(" AND "));

    return context.nested("FILTER IN MEMORY: " + expressionDesc);
  }
}
