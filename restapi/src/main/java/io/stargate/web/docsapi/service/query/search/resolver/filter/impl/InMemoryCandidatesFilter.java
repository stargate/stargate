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

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.Predicate;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.search.db.impl.DocumentSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterPathSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import io.stargate.web.rx.RxUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {{@link CandidatesFilter} that works with set of {@link FilterExpression}s that contain only
 * in-memory conditions.
 */
public class InMemoryCandidatesFilter implements CandidatesFilter {

  private final Collection<FilterExpression> expressions;

  private final FilterPathSearchQueryBuilder queryBuilder;

  private final ExecutionContext context;

  private InMemoryCandidatesFilter(
      Collection<FilterExpression> expressions, ExecutionContext context) {
    boolean hasPersistence =
        expressions.stream().anyMatch(e -> e.getCondition().isPersistenceCondition());

    if (hasPersistence) {
      throw new IllegalArgumentException(
          "InMemoryCandidatesDocumentsResolver works only with the non persistence conditions.");
    }

    this.expressions = expressions;
    this.queryBuilder = new DocumentSearchQueryBuilder(expressions);
    this.context = createContext(context, expressions);
  }

  public static Function<ExecutionContext, CandidatesFilter> forExpression(
      FilterExpression expression) {
    return forExpressions(Collections.singletonList(expression));
  }

  public static Function<ExecutionContext, CandidatesFilter> forExpressions(
      Collection<FilterExpression> expressions) {
    return context -> new InMemoryCandidatesFilter(expressions, context);
  }

  @Override
  public Single<? extends Query<? extends BoundQuery>> prepareQuery(
      DataStore dataStore, DocsApiConfiguration configuration, String keyspace, String collection) {
    FilterPath filterPath = queryBuilder.getFilterPath();
    // resolve depth we need
    String[] neededColumns =
        QueryConstants.ALL_COLUMNS_NAMES.apply(filterPath.getPath().size() + 1);
    // we can only fetch one row if path is fixed
    Integer limit = filterPath.isFixed() ? 1 : null;
    return RxUtils.singleFromFuture(
            () -> {
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(
                      dataStore::queryBuilder, keyspace, collection, limit, neededColumns);
              return dataStore.prepare(query);
            })
        .cache();
  }

  @Override
  public Maybe<?> bindAndFilter(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      Query<? extends BoundQuery> preparedQuery,
      RawDocument document) {
    BoundQuery query = preparedQuery.bind(document.id());

    // query, take one, test against expression
    FilterPath filterPath = queryBuilder.getFilterPath();

    // page size 2 with limit 1 to ensure no extra page fetching (only on fixed path)
    int pageSize = filterPath.isFixed() ? 2 : configuration.getSearchPageSize();
    return queryExecutor
        .queryDocs(query, pageSize, null, context)
        .take(1)
        .map(RawDocument::rows)
        .switchIfEmpty(
            Flowable.defer(
                () -> {
                  // check if we might have only evaluate on missing
                  boolean allEvalOnMissing =
                      expressions.stream()
                          .allMatch(e -> e.getCondition().isEvaluateOnMissingFields());

                  // if so, pass empty row list here, so we test against this
                  // otherwise keep empty
                  if (allEvalOnMissing) {
                    return Flowable.just(Collections.emptyList());
                  } else {
                    return Flowable.empty();
                  }
                }))
        .filter(matchAll(expressions))
        .singleElement();
  }

  private Predicate<? super List<Row>> matchAll(Collection<FilterExpression> expressions) {
    return documentRows -> {
      for (FilterExpression expression : expressions) {
        if (!expression.test(documentRows)) {
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
