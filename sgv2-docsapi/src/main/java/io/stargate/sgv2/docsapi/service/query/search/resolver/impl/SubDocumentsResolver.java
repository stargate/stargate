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

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.eval.EvalEngine;
import com.bpodgursky.jbool_expressions.eval.EvalRule;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.eval.RawDocumentEvalRule;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.SubDocumentSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
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

  private final DocumentProperties documentProperties;

  public SubDocumentsResolver(
      Expression<FilterExpression> expression,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context,
      DocumentProperties documentProperties) {
    this.expression = expression;
    this.context = createContext(context, subDocumentPath);
    this.queryBuilder =
        new SubDocumentSearchQueryBuilder(documentProperties, documentId, subDocumentPath);
    // key depth explained:
    //  - one extra for the document id
    this.keyDepth = subDocumentPath.size() + 1;
    this.documentProperties = documentProperties;
  }

  @Override
  public Multi<RawDocument> getDocuments(
      QueryExecutor queryExecutor, String keyspace, String collection, Paginator paginator) {

    // todo optimize
    String[] columns =
        documentProperties.tableColumns().allColumns().stream()
            .map(Column::name)
            .toArray(String[]::new);

    // prepare the query
    return Uni.createFrom()
        .item(() -> queryBuilder.buildQuery(keyspace, collection, columns))

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
              // go for max storage page size as we have the document
              return queryExecutor.queryDocs(
                  keyDepth,
                  query,
                  documentProperties.getApproximateStoragePageSize(paginator.docPageSize),
                  true,
                  paginator.getCurrentDbPageState(),
                  true,
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
    return context.nested(
        "SearchSubDocuments: sub-path '"
            + String.join(".", prependPath)
            + "', expression: '"
            + expression.toString()
            + "'");
  }
}
