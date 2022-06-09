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

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.DocumentSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.filter.CandidatesFilter;
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

  private final DocumentProperties documentProperties;

  private PersistenceCandidatesFilter(
      Collection<FilterExpression> expressions,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    boolean hasInMemory =
        expressions.stream().anyMatch(e -> !e.getCondition().isPersistenceCondition());

    if (hasInMemory) {
      throw new IllegalArgumentException(
          "PersistenceCandidatesDocumentsResolver works only with the persistence conditions.");
    }

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
    return context -> new PersistenceCandidatesFilter(expressions, context, documentProperties);
  }

  @Override
  public Uni<QueryOuterClass.Query> prepareQuery(String keyspace, String collection) {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    return Uni.createFrom()
        .item(
            () -> {
              FilterPath filterPath = queryBuilder.getFilterPath();
              Integer limit = filterPath.isFixed() ? 1 : null;
              return queryBuilder.buildQuery(
                  keyspace,
                  collection,
                  limit,
                  tableProps.keyColumnName(),
                  tableProps.leafColumnName());
            })
        .memoize()
        .indefinitely();
  }

  @Override
  public Uni<Boolean> bindAndFilter(
      QueryExecutor queryExecutor, QueryOuterClass.Query preparedQuery, RawDocument document) {
    QueryOuterClass.Value idValue = Values.of(document.id());
    QueryOuterClass.Query query = queryBuilder.bindWithValues(preparedQuery, idValue);

    // execute query
    // page size 2 with limit 1 to ensure no additional pages fetched (only on fixed path)
    // use max storage page size otherwise as we have the doc id
    FilterPath filterPath = queryBuilder.getFilterPath();
    int pageSize = filterPath.isFixed() ? 2 : documentProperties.maxSearchPageSize();
    return queryExecutor
        .queryDocs(query, pageSize, false, null, false, context)

        // select only first one
        .select()
        .first()

        // if we have an item report true, otherwise on empty false
        .onItem()
        .transform(any -> true)
        .onCompletion()
        .ifEmpty()
        .continueWith(false)

        // and then convert to uni that will always emit
        .toUni();
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
