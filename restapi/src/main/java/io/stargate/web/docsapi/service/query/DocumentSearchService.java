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

package io.stargate.web.docsapi.service.query;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.PopulateSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.BaseResolver;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.rx.RxUtils;
import java.util.Collection;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

public class DocumentSearchService {

  @Inject private DocsApiConfiguration configuration;

  public Flowable<RawDocument> searchDocuments(
      QueryExecutor queryExecutor,
      String keyspace,
      String collection,
      Expression<FilterExpression> expression,
      Collection<String> fields,
      Paginator paginator,
      ExecutionContext context) {

    // if we have true immediately, means we can only do full search
    if (Literal.EXPR_TYPE.equals(expression.getExprType())) {

      // for the sake of correctness make sure we don't have have false
      if (Literal.getFalse().equals(expression)) {
        return Flowable.empty();
      }

      // fetch docs, no need to populate as all rows are taken
      return fullSearch(
              queryExecutor,
              configuration,
              keyspace,
              collection,
              paginator,
              nestedFullSearch(context))

          // load only for the page size
          .take((paginator.docPageSize));
    } else {
      // otherwise resolve the expression
      DocumentsResolver documentsResolver = BaseResolver.resolve(expression, context);

      // load the candidates
      Flowable<RawDocument> candidates =
          documentsResolver
              .getDocuments(queryExecutor, configuration, keyspace, collection, paginator)

              // limit to requested page size only to stop fetching extra docs
              .take(paginator.docPageSize);

      // then populate
      return populateCandidates(
          candidates, queryExecutor, keyspace, collection, nestedPopulate(context));
    }
  }

  public Flowable<RawDocument> fullSearch(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      Paginator paginator,
      ExecutionContext context) {

    // TODO use fields when having them to limit scope of fetched columns
    //  plus add the p(depth+1) = "" to not fetch rows we don't need

    // prepare first (this could be cached for the max depth)
    return RxUtils.singleFromFuture(
            () -> {
              String[] columns =
                  QueryConstants.ALL_COLUMNS_NAMES.apply(configuration.getMaxDepth());

              DataStore dataStore = queryExecutor.getDataStore();

              FullSearchQueryBuilder queryBuilder = new FullSearchQueryBuilder();
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection, columns);

              return dataStore.prepare(query);
            })
        .cache()
        .flatMapPublisher(
            prepared -> {
              BoundQuery boundQuery = prepared.bind();
              return queryExecutor.queryDocs(
                  boundQuery,
                  configuration.getSearchPageSize(),
                  paginator.getCurrentDbPageState(),
                  context);
            });
  }

  // populates the given documents by using a prepared query
  private Flowable<RawDocument> populateCandidates(
      Flowable<RawDocument> candidates,
      QueryExecutor queryExecutor,
      String keyspace,
      String collection,
      ExecutionContext context) {

    // prepare query
    Single<? extends Query<? extends BoundQuery>> preparedSingle =
        RxUtils.singleFromFuture(
                () -> {
                  // TODO make sure that if we have fields, we only fetch max amount of P columns
                  // needed
                  //  plus add the p(depth+1) = "" to not fetch rows we don't need
                  //  fallback to max depth if no fields
                  //  not possible at the moment, as JsonConverter is not supporting it
                  // long depth =
                  // DocumentServiceUtils.maxFieldsDepth(fields).orElse(configuration.getMaxDepth());

                  // columns from depth
                  String[] columns =
                      QueryConstants.ALL_COLUMNS_NAMES.apply(
                          Long.valueOf(configuration.getMaxDepth()).intValue());

                  // data store need for build and prepare
                  DataStore dataStore = queryExecutor.getDataStore();

                  // build and prepare
                  PopulateSearchQueryBuilder queryBuilder = new PopulateSearchQueryBuilder();
                  BuiltQuery<? extends BoundQuery> query =
                      queryBuilder.buildQuery(
                          dataStore::queryBuilder, keyspace, collection, columns);

                  return dataStore.prepare(query);
                })
            // then cache so we can reuse for each document
            .cache();

    // combine
    return candidates
        .withLatestFrom(preparedSingle.toFlowable(), Pair::of)
        .concatMap(
            p -> {
              // bind for this doc id
              RawDocument document = p.getLeft();
              BoundQuery query = p.getRight().bind(document.id());

              // fetch, take one and then populate into the original doc
              return queryExecutor
                  .queryDocs(query, configuration.getSearchPageSize(), null, context)
                  .firstElement()
                  .map(document::populateFrom)
                  .toFlowable();
            });
  }

  private ExecutionContext nestedPopulate(ExecutionContext context) {
    return context.nested("LoadProperties");
  }

  private ExecutionContext nestedFullSearch(ExecutionContext context) {
    return context.nested("LoadAllDocuments");
  }
}
