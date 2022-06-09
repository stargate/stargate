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
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
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

  private final DocumentProperties documentProperties;

  public PersistenceDocumentsResolver(
      FilterExpression expression,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    this(Collections.singletonList(expression), context, documentProperties);
  }

  public PersistenceDocumentsResolver(
      Collection<FilterExpression> expressions,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    boolean hasInMemory =
        expressions.stream().anyMatch(e -> !e.getCondition().isPersistenceCondition());

    if (hasInMemory) {
      throw new IllegalArgumentException(
          "PersistenceDocumentsResolver works only with the persistence conditions.");
    }

    this.queryBuilder = new FilterExpressionSearchQueryBuilder(documentProperties, expressions);
    this.context = createContext(context, expressions);
    this.documentProperties = documentProperties;
  }

  /** {@inheritDoc} */
  @Override
  public Multi<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {

    DocumentTableProperties tableProperties = documentProperties.tableProperties();

    // prepare the query
    return Uni.createFrom()
        .item(
            () ->
                queryBuilder.buildQuery(
                    keyspace,
                    collection,
                    tableProperties.keyColumnName(),
                    tableProperties.leafColumnName()))

        // cache the prepared
        .memoize()
        .indefinitely()

        // then bind and execute
        .onItem()
        .transformToMulti(
            built -> {
              // bind (no values needed)
              QueryOuterClass.Query query = queryBuilder.bind(built);

              // execute by respecting the paging state
              // take always one more than needed to stop pre-fetching
              // use exponential page size to increase when more is needed
              int pageSize = paginator.docPageSize + 1;
              // fetch paging as this can be the first resolver in chain
              boolean fetchRowPaging = true;
              return queryExecutor.queryDocs(
                  query,
                  pageSize,
                  true,
                  paginator.getCurrentDbPageState(),
                  fetchRowPaging,
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
