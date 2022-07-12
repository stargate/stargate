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

package io.stargate.sgv2.docsapi.service.query.search.resolver.filter.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.DocumentSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FilterPathSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.CandidatesFilter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@link CandidatesFilter} that works with set of {@link FilterExpression}s that contain only
 * in-memory conditions.
 */
public class InMemoryCandidatesFilter implements CandidatesFilter {

  private final Collection<FilterExpression> expressions;

  private final FilterPathSearchQueryBuilder queryBuilder;

  private final ExecutionContext context;

  private final DocumentProperties documentProperties;

  private InMemoryCandidatesFilter(
      Collection<FilterExpression> expressions,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    boolean hasPersistence =
        expressions.stream().anyMatch(e -> e.getCondition().isPersistenceCondition());

    if (hasPersistence) {
      throw new IllegalArgumentException(
          "InMemoryCandidatesDocumentsResolver works only with the non persistence conditions.");
    }

    this.expressions = expressions;
    this.queryBuilder = new DocumentSearchQueryBuilder(documentProperties, expressions);
    this.context = createContext(context, expressions);
    this.documentProperties = documentProperties;
  }

  public static Function<ExecutionContext, CandidatesFilter> forExpression(
      FilterExpression expression, DocumentProperties documentProperties) {
    return forExpressions(Collections.singletonList(expression), documentProperties);
  }

  public static Function<ExecutionContext, CandidatesFilter> forExpressions(
      Collection<FilterExpression> expressions, DocumentProperties documentProperties) {
    return context -> new InMemoryCandidatesFilter(expressions, context, documentProperties);
  }

  @Override
  public Uni<QueryOuterClass.Query> prepareQuery(String keyspace, String collection) {
    FilterPath filterPath = queryBuilder.getFilterPath();
    // resolve depth we need
    // TODO optimize
    String[] neededColumns =
        documentProperties
            .tableColumns()
            .allColumnNamesWithPathDepth(filterPath.getPath().size() + 1)
            .toArray(String[]::new);

    // we can only fetch one row if path is fixed
    Integer limit = filterPath.isFixed() ? 1 : null;
    return Uni.createFrom()
        .item(() -> queryBuilder.buildQuery(keyspace, collection, limit, neededColumns))
        .memoize()
        .indefinitely();
  }

  @Override
  public Uni<Boolean> bindAndFilter(
      QueryExecutor queryExecutor, QueryOuterClass.Query preparedQuery, RawDocument document) {
    QueryOuterClass.Value idValue = Values.of(document.id());
    QueryOuterClass.Query query = queryBuilder.bindWithValues(preparedQuery, idValue);

    // query, take one, test against expression
    FilterPath filterPath = queryBuilder.getFilterPath();

    // page size 2 with limit 1 to ensure no extra page fetching (only on fixed path)
    // use max storage page size otherwise as we have the doc id
    int pageSize = filterPath.isFixed() ? 2 : documentProperties.maxSearchPageSize();
    return queryExecutor
        .queryDocs(query, pageSize, false, null, false, context)

        // get first
        .select()
        .first()

        // map to rows
        .map(d -> matchAll(expressions).test(d.rows()))

        // handle case with no results
        .onCompletion()
        .ifEmpty()
        .switchTo(
            Multi.createFrom()
                .deferred(
                    () -> {
                      // check if we might have only evaluate on missing
                      boolean allEvalOnMissing =
                          expressions.stream()
                              .allMatch(e -> e.getCondition().isEvaluateOnMissingFields());

                      // if so, pass empty row list here, so we test against this
                      // otherwise keep empty
                      if (allEvalOnMissing) {
                        boolean test = matchAll(expressions).test(Collections.emptyList());
                        return Multi.createFrom().items(test);
                      } else {
                        return Multi.createFrom().items(false);
                      }
                    }))

        // and then convert to uni that will always emit
        .toUni();
  }

  private Predicate<? super List<RowWrapper>> matchAll(Collection<FilterExpression> expressions) {
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
