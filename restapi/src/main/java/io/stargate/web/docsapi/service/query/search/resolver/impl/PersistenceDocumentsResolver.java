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
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.rx.RxUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * {@link DocumentsResolver} that works with set of {@link FilterExpression}s that are on the same
 * path containing only persistence conditions.
 */
public class PersistenceDocumentsResolver implements DocumentsResolver {

  private final AbstractSearchQueryBuilder queryBuilder;

  private final ExecutionContext context;

  public PersistenceDocumentsResolver(FilterExpression expression, ExecutionContext context) {
    this(Collections.singletonList(expression), context);
  }

  public PersistenceDocumentsResolver(
      Collection<FilterExpression> expressions, ExecutionContext context) {
    boolean hasInMemory =
        expressions.stream().anyMatch(e -> !e.getCondition().isPersistenceCondition());

    if (hasInMemory) {
      throw new IllegalArgumentException(
          "PersistenceDocumentsResolver works only with the persistence conditions.");
    }

    this.queryBuilder = new FilterExpressionSearchQueryBuilder(expressions);
    this.context = createContext(context, expressions);
  }

  /** {@inheritDoc} */
  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      Paginator paginator) {

    // prepare the query
    return RxUtils.singleFromFuture(
            () -> {
              DataStore dataStore = queryExecutor.getDataStore();
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(
                      dataStore::queryBuilder,
                      keyspace,
                      collection,
                      QueryConstants.KEY_COLUMN_NAME,
                      QueryConstants.LEAF_COLUMN_NAME);
              return dataStore.prepare(query);
            })

        // cache the prepared
        .cache()
        .flatMapPublisher(
            prepared -> {
              // bind (no values needed)
              BoundQuery query = prepared.bind();

              // execute by respecting the paging state
              return queryExecutor.queryDocs(
                  query,
                  paginator.docPageSize
                      + 1, // take always one more than needed to stop pre-fetching
                  true,
                  paginator.getCurrentDbPageState(),
                  context);
            });
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
