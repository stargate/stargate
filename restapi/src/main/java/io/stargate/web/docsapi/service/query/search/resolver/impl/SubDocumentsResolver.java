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
import io.stargate.web.docsapi.service.query.eval.EvalFilterExpression;
import io.stargate.web.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.SubDocumentSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.rx.RxUtils;
import java.util.List;
import java.util.Map;

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
    this.expression = expression;
    this.context = createContext(context, subDocumentPath);
    this.queryBuilder = new SubDocumentSearchQueryBuilder(documentId, subDocumentPath);
    // key depth explained:
    //  - one extra for the document id
    //  - one extra on size of sub-document path to distinguish ids after sub-document path
    this.keyDepth = subDocumentPath.size() + 2;
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
              rules.put(FilterExpression.EXPR_TYPE, new EvalFilterExpression(document));
              return EvalEngine.evaluate(expression, rules);
            });
  }

  private ExecutionContext createContext(ExecutionContext context, List<String> prependPath) {
    return context.nested("LoadSubDocuments: sub-path '" + String.join(".", prependPath) + "'");
  }
}
