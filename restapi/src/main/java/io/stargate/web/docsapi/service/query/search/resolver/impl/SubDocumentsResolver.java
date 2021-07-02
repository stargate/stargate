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

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.eval.EvalEngine;
import com.bpodgursky.jbool_expressions.eval.EvalRule;
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
import io.stargate.web.docsapi.service.query.eval.RawDocumentEvalRule;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.SubDocumentSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.rx.RxUtils;
import java.util.List;
import java.util.Map;

/**
 * A document resolver that loads full (sub-)documents on the given sub-path that match an
 * Expression.
 *
 * <p>Note that this resolver has two modes, controlled by the #splitOnSubPathKeys constructor
 * parameters:
 *
 * <ol>
 *   <li>1. <code>false</code> - there is no split by the keys on the sub-path, effectively the
 *       whole document on the sub-path is returned
 *   <li>2. <code>true</code> - there is extra split by the key values in the doc on the sub-path,
 *       effectively returns document per key
 * </ol>
 */
public class SubDocumentsResolver implements DocumentsResolver {

  private final Expression<FilterExpression> expression;

  private final AbstractSearchQueryBuilder queryBuilder;

  private final ExecutionContext context;

  private final int keyDepth;

  public SubDocumentsResolver(
      Expression<FilterExpression> expression,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {
    this(expression, documentId, subDocumentPath, context, false);
  }

  public SubDocumentsResolver(
      Expression<FilterExpression> expression,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context,
      boolean splitOnSubPathKeys) {
    this.expression = expression;
    this.context = createContext(context, subDocumentPath);
    this.queryBuilder = new SubDocumentSearchQueryBuilder(documentId, subDocumentPath);
    // key depth explained:
    //  - one extra for the document id
    //  - one extra on size if we need to split based on keys ids after sub-document path
    this.keyDepth = subDocumentPath.size() + (splitOnSubPathKeys ? 2 : 1);
  }

  @Override
  public Flowable<RawDocument> getDocuments(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      Paginator paginator) {
    String[] columns = QueryConstants.ALL_COLUMNS_NAMES.apply(configuration.getMaxDepth());

    // prepare the query
    return RxUtils.singleFromFuture(
            () -> {
              DataStore dataStore = queryExecutor.getDataStore();
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection, columns);
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
                  keyDepth,
                  query,
                  configuration.getSearchPageSize(),
                  paginator.getCurrentDbPageState(),
                  context);
            })
        .filter(
            document -> {
              Map<String, EvalRule<FilterExpression>> rules = EvalEngine.booleanRules();
              rules.put(FilterExpression.EXPR_TYPE, new RawDocumentEvalRule(document));
              return EvalEngine.evaluate(expression, rules);
            });
  }

  private ExecutionContext createContext(ExecutionContext context, List<String> prependPath) {
    return context.nested("LoadSubDocuments: sub-path '" + String.join(".", prependPath) + "'");
  }
}
