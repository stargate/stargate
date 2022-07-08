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

package io.stargate.sgv2.docsapi.service.query.search.resolver.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FilterPathSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Predicate;
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

  private final DocumentProperties documentProperties;

  public InMemoryDocumentsResolver(
      FilterExpression expression,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    this(Collections.singletonList(expression), context, documentProperties);
  }

  public InMemoryDocumentsResolver(
      Collection<FilterExpression> expressions,
      ExecutionContext context,
      DocumentProperties documentProperties) {
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
            ? new FullSearchQueryBuilder(documentProperties)
            : new FilterExpressionSearchQueryBuilder(documentProperties, expressions);
    this.context = createContext(context, expressions);
    this.documentProperties = documentProperties;
  }

  /** {@inheritDoc} */
  @Override
  public Multi<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {

    // if we have a filter path query then need all the columns on the filter, plus one additional
    // to match, otherwise we need all columns
    String[] neededColumns =
        Optional.of(queryBuilder)
            .filter(FilterPathSearchQueryBuilder.class::isInstance)
            .map(FilterPathSearchQueryBuilder.class::cast)
            .map(qb -> qb.getFilterPath().getPath().size() + 1)
            .map(
                depth ->
                    documentProperties
                        .tableColumns()
                        .allColumnNamesWithPathDepth(depth)
                        .toArray(String[]::new))
            .orElse(documentProperties.tableColumns().allColumnNamesArray());

    // bind and build the query
    return Uni.createFrom()
        .item(
            () -> {
              QueryOuterClass.Query query =
                  queryBuilder.buildQuery(keyspace, collection, neededColumns);
              return queryBuilder.bind(query);
            })

        // cache it
        .memoize()
        .indefinitely()

        // then bind and execute
        .onItem()
        .transformToMulti(
            query -> {
              // in case we have a full search then base the page size on the app. doc size
              // otherwise start with the requested page size, plus one more than needed to stop
              // pre-fetching
              int pageSize =
                  evaluateOnMissing
                      ? documentProperties.getApproximateStoragePageSize(paginator.docPageSize)
                      : paginator.docPageSize + 1;
              // fetch paging as this can be the first resolver in chain
              boolean fetchRowPaging = true;
              return queryExecutor.queryDocs(
                  query,
                  pageSize,
                  true,
                  paginator.getCurrentDbPageState(),
                  fetchRowPaging,
                  context);
            })

        // then filter to match the expression (in-memory filters have no predicates on the values)
        .select()
        .where(matchAll(expressions));
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
