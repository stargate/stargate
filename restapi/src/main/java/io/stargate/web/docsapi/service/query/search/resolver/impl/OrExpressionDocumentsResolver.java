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
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.eval.RawDocumentEvalRule;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterExpressionSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FilterPathSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.docsapi.service.query.search.weigth.impl.UserOrderWeightResolver;
import io.stargate.web.rx.RxUtils;
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

  public OrExpressionDocumentsResolver(Or<FilterExpression> expression, ExecutionContext context) {
    this.expression = expression;
    this.queryBuilders = buildQueries(getChildren(expression));
    this.context = createContext(context, expression);
  }

  /** {@inheritDoc} */
  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      Paginator paginator) {
    DataStore dataStore = queryExecutor.getDataStore();

    // find the max size of columns in all queries and use that for now
    String[] columns =
        queryBuilders.stream()
            .map(qb -> columnsForQuery(qb, configuration.getMaxDepth()))
            .max(Comparator.comparingInt(o -> o.length))
            .orElseGet(() -> QueryConstants.ALL_COLUMNS_NAMES.apply(configuration.getMaxDepth()));

    // resolve if no path are there, used in the filtering
    boolean noPaths =
        Arrays.stream(columns)
            .noneMatch(c -> Objects.equals(c, QueryConstants.P_COLUMN_NAME.apply(0)));

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
              return queryExecutor.queryDocs(
                  boundQueries,
                  configuration.getSearchPageSize(),
                  paginator.getCurrentDbPageState(),
                  context);
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
      columns = new String[] {QueryConstants.KEY_COLUMN_NAME, QueryConstants.LEAF_COLUMN_NAME};
    } else if (queryBuilder instanceof FilterPathSearchQueryBuilder) {
      FilterPathSearchQueryBuilder fpqb = (FilterPathSearchQueryBuilder) queryBuilder;
      columns = QueryConstants.ALL_COLUMNS_NAMES.apply(fpqb.getFilterPath().getPath().size() + 1);
    } else {
      columns = QueryConstants.ALL_COLUMNS_NAMES.apply(maxDepth);
    }
    return columns;
  }

  private List<AbstractSearchQueryBuilder> buildQueries(List<FilterExpression> children) {
    // if any condition is evaluated on missing, then we can do a full search only
    boolean evaluateOnMissing =
        children.stream().anyMatch(e -> e.getCondition().isEvaluateOnMissingFields());
    if (evaluateOnMissing) {
      return Collections.singletonList(new FullSearchQueryBuilder());
    }

    // otherwise, for each persistence condition an own builder
    List<AbstractSearchQueryBuilder> persistenceQueries =
        children.stream()
            .filter(e -> e.getCondition().isPersistenceCondition())
            .map(FilterExpressionSearchQueryBuilder::new)
            .collect(Collectors.toList());

    // for the memory ones, we can collect only distinct filter paths
    List<AbstractSearchQueryBuilder> inMemoryQueries =
        children.stream()
            .filter(e -> !e.getCondition().isPersistenceCondition())
            .map(FilterExpression::getFilterPath)
            .distinct()
            .map(fp -> new FilterPathSearchQueryBuilder(fp, true))
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
    return context.nested("MERGING OR: expression '" + expression + "'");
  }
}
