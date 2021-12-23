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

import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.eval.EvalEngine;
import com.bpodgursky.jbool_expressions.eval.EvalRule;
import io.reactivex.rxjava3.core.Flowable;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.eval.RawDocumentEvalRule;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterPathSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.docsapi.service.query.search.weigth.impl.UserOrderWeightResolver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class OrExpressionDocumentsResolver implements DocumentsResolver {

  private final Or<FilterExpression> expression;

  private final List<AbstractSearchQueryBuilder> queryBuilders;

  private final ExecutionContext context;

  private final boolean evaluateOnMissing;

  private final DocsApiConfiguration config;

  public OrExpressionDocumentsResolver(
      Or<FilterExpression> expression, ExecutionContext context, DocsApiConfiguration config) {
    List<FilterExpression> children = getChildren(expression);

    this.expression = expression;
    this.evaluateOnMissing =
        children.stream().anyMatch(e -> e.getCondition().isEvaluateOnMissingFields());
    this.config = config;
    this.queryBuilders = buildQueries(evaluateOnMissing, children);
    this.context = createContext(context, expression);
  }

  /** {@inheritDoc} */
  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {
    DataStore dataStore = queryExecutor.getDataStore();

    // find the max size of columns in all queries and use that for now
    String[] columns =
        queryBuilders.stream()
            .map(qb -> columnsForQuery(qb, config.getMaxDepth()))
            .max(Comparator.comparingInt(o -> o.length))
            .orElseGet(() -> DocsApiConstants.ALL_COLUMNS_NAMES.apply(config.getMaxDepth()));

    // resolve if no path are there, used in the filtering
    boolean noPaths =
        Arrays.stream(columns)
            .noneMatch(c -> Objects.equals(c, DocsApiConstants.P_COLUMN_NAME.apply(0)));

    return Flowable.fromIterable(queryBuilders)
        .concatMap(
            queryBuilder ->
                RxUtils.singleFromFuture(
                        () -> {
                          BuiltQuery<? extends BoundQuery> query =
                              queryBuilder.buildQuery(
                                  dataStore::queryBuilder, keyspace, collection, columns);
                          return dataStore.prepare(query);
                        })
                    .toFlowable())
        .toList()
        .cache()
        .flatMapPublisher(
            preparedQueries -> {
              // find all with empty bind values
              List<BoundQuery> boundQueries =
                  preparedQueries.stream().map(Query::bind).collect(Collectors.toList());

              // execute them all by respecting the paging state
              // if we have evaluate on missing then we have single query, so go for the
              // getStoragePageSize
              // otherwise the page size for each query should be requested page size + 1
              // since we are doing in order merge of the results to preserve sorting
              // we can not fetch fewer document
              int pageSize =
                  evaluateOnMissing
                      ? config.getApproximateStoragePageSize(paginator.docPageSize)
                      : paginator.docPageSize + 1;
              return queryExecutor.queryDocs(
                  boundQueries, pageSize, true, paginator.getCurrentDbPageState(), context);
            })
        .filter(
            doc -> {
              // now we can get doc as a result of the persistence or in memory query
              // if we only run persistence queries we can return true immediately
              // running only persistence queries means we had no path columns
              if (noPaths) {
                return true;
              }

              // otherwise evaluate using the EvalEngine
              // this is gonna test the persistence expressions as well, but this is fine
              Map<String, EvalRule<FilterExpression>> rules = EvalEngine.booleanRules();
              rules.put(FilterExpression.EXPR_TYPE, new RawDocumentEvalRule(doc));
              return EvalEngine.evaluate(expression, rules);
            });
  }

  private String[] columnsForQuery(AbstractSearchQueryBuilder queryBuilder, int maxDepth) {
    String[] columns;
    if (queryBuilder instanceof FilterExpressionSearchQueryBuilder) {
      columns = new String[] {DocsApiConstants.KEY_COLUMN_NAME, DocsApiConstants.LEAF_COLUMN_NAME};
    } else if (queryBuilder instanceof FilterPathSearchQueryBuilder) {
      FilterPathSearchQueryBuilder fpqb = (FilterPathSearchQueryBuilder) queryBuilder;
      columns = DocsApiConstants.ALL_COLUMNS_NAMES.apply(fpqb.getFilterPath().getPath().size() + 1);
    } else {
      columns = DocsApiConstants.ALL_COLUMNS_NAMES.apply(maxDepth);
    }
    return columns;
  }

  private List<AbstractSearchQueryBuilder> buildQueries(
      boolean evaluateOnMissing, List<FilterExpression> children) {
    // if any condition is evaluated on missing, then we can do a full search only
    if (evaluateOnMissing) {
      return Collections.singletonList(new FullSearchQueryBuilder());
    }

    // otherwise, for each persistence condition an own builder
    List<AbstractSearchQueryBuilder> persistenceQueries =
        children.stream()
            .filter(e -> e.getCondition().isPersistenceCondition())
            .map(
                isPersistenceCondition ->
                    new FilterExpressionSearchQueryBuilder(isPersistenceCondition, config))
            .collect(Collectors.toList());

    // for the memory ones, we can collect only distinct filter paths
    List<AbstractSearchQueryBuilder> inMemoryQueries =
        children.stream()
            .filter(e -> !e.getCondition().isPersistenceCondition())
            .map(FilterExpression::getFilterPath)
            .distinct()
            .map(fp -> new FilterPathSearchQueryBuilder(fp, true, config))
            .collect(Collectors.toList());

    // merge and return
    persistenceQueries.addAll(inMemoryQueries);
    return persistenceQueries;
  }

  private List<FilterExpression> getChildren(Or<FilterExpression> expression) {
    // collect first
    Set<FilterExpression> set = new HashSet<>();
    expression.collectK(set, Integer.MAX_VALUE);

    // then always maintain a same order by sorting
    UserOrderWeightResolver resolver = UserOrderWeightResolver.of();
    List<FilterExpression> result = new ArrayList<>(set);
    result.sort(resolver::compare);
    return result;
  }

  private ExecutionContext createContext(
      ExecutionContext context, Or<FilterExpression> expression) {
    // Note: use toLexicographicString to ensure a stable order of sub-expressions.
    return context.nested("MERGING OR: expression '" + expression.toLexicographicString() + "'");
  }
}
