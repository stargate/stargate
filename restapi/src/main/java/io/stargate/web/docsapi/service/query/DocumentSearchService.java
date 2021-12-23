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
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.PopulateSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.db.impl.SubDocumentSearchQueryBuilder;
import io.stargate.web.docsapi.service.query.search.resolver.BaseResolver;
import io.stargate.web.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.web.docsapi.service.query.search.resolver.impl.SubDocumentsResolver;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

public class DocumentSearchService {

  @Inject private DocsApiConfiguration configuration;

  /**
   * Searches a complete collection in order to find the documents that match the given expression.
   * Starts the search for the given {@link Paginator} state.
   *
   * @param queryExecutor Query executor for running queries.
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param expression Expression tree
   * @param paginator {@link Paginator}
   * @param context Context for recording profiling information
   * @return Flowable of {@link RawDocument}s.
   */
  public Flowable<RawDocument> searchDocuments(
      QueryExecutor queryExecutor,
      String keyspace,
      String collection,
      Expression<FilterExpression> expression,
      Paginator paginator,
      ExecutionContext context) {

    // if we have true immediately, means we can only do full search
    if (Literal.EXPR_TYPE.equals(expression.getExprType())) {

      // for the sake of correctness make sure we don't have a false
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
          .take(paginator.docPageSize);
    } else {
      // otherwise resolve the expression
      DocumentsResolver documentsResolver =
          BaseResolver.resolve(expression, context, configuration);

      // load the candidates
      Flowable<RawDocument> candidates =
          documentsResolver
              .getDocuments(queryExecutor, keyspace, collection, paginator)

              // limit to requested page size only to stop fetching extra docs
              .take(paginator.docPageSize);

      // then populate
      return populateCandidates(
          candidates, queryExecutor, keyspace, collection, nestedPopulate(context));
    }
  }

  /**
   * Searches a single document in order to find the sub-documents that match the given expression.
   * Sub-documents are defined by the #subDocumentPath, everything outside this path is ignored.
   * Starts the search for the given {@link Paginator} state.
   *
   * @param queryExecutor Query executor for running queries.
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param documentId Document ID to search in
   * @param subDocumentPath Path where to find sub-documents
   * @param expression Expression tree to fulfill (note that #subDocumentPath must be already
   *     included in the {@link FilterExpression}s)
   * @param paginator {@link Paginator}
   * @param context Context for recording profiling information
   * @return Flowable of {@link RawDocument}s representing sub-documents in the given
   *     #subDocumentPath.
   */
  public Flowable<RawDocument> searchSubDocuments(
      QueryExecutor queryExecutor,
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      Expression<FilterExpression> expression,
      Paginator paginator,
      ExecutionContext context) {

    // for the sake of correctness make sure we don't have have false
    if (Literal.getFalse().equals(expression)) {
      return Flowable.empty();
    }

    // create the resolver and return results
    SubDocumentsResolver subDocumentsResolver =
        new SubDocumentsResolver(expression, documentId, subDocumentPath, context, configuration);
    return subDocumentsResolver
        .getDocuments(queryExecutor, keyspace, collection, paginator)

        // limit to requested page size only to stop fetching extra docs
        .take(paginator.docPageSize);
  }

  /**
   * Gets a single document optionally limit to the the #subDocumentPath.
   *
   * @param queryExecutor Query executor for running queries.
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param documentId Document ID to search in
   * @param subDocumentPath Path where to find the document
   * @param context Context for recording profiling information
   * @return Flowable of {@link RawDocument}s representing a document or sub-document in the given
   *     #subDocumentPath.
   */
  public Flowable<RawDocument> getDocument(
      QueryExecutor queryExecutor,
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {

    return fullDocument(
            queryExecutor,
            configuration,
            keyspace,
            collection,
            documentId,
            subDocumentPath,
            nestedFullDocument(context))

        // take one, as there can be only one document
        .take(1);
  }

  private Flowable<RawDocument> fullSearch(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      Paginator paginator,
      ExecutionContext context) {

    // prepare first (this could be cached for the max depth)
    return RxUtils.singleFromFuture(
            () -> {
              int maxDepth = configuration.getMaxDepth();
              String[] columns = DocsApiConstants.ALL_COLUMNS_NAMES.apply(maxDepth);

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
                  configuration.getApproximateStoragePageSize(paginator.docPageSize),
                  true,
                  paginator.getCurrentDbPageState(),
                  context);
            });
  }

  private Flowable<RawDocument> fullDocument(
      QueryExecutor queryExecutor,
      DocsApiConfiguration configuration,
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {

    // prepare first
    return RxUtils.singleFromFuture(
            () -> {
              int maxDepth = configuration.getMaxDepth();
              String[] columns = DocsApiConstants.ALL_COLUMNS_NAMES.apply(maxDepth);

              DataStore dataStore = queryExecutor.getDataStore();

              SubDocumentSearchQueryBuilder queryBuilder =
                  new SubDocumentSearchQueryBuilder(documentId, subDocumentPath);
              BuiltQuery<? extends BoundQuery> query =
                  queryBuilder.buildQuery(dataStore::queryBuilder, keyspace, collection, columns);

              return dataStore.prepare(query);
            })
        .cache()
        .flatMapPublisher(
            prepared -> {
              // since we have the doc id, use the max storage page size to grab all the rows for
              // that doc
              BoundQuery boundQuery = prepared.bind();
              return queryExecutor.queryDocs(
                  boundQuery, configuration.getMaxStoragePageSize(), false, null, context);
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
                  // columns from depth
                  int maxDepth = configuration.getMaxDepth();
                  String[] columns = DocsApiConstants.ALL_COLUMNS_NAMES.apply(maxDepth);

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
        .concatMapSingle(doc -> preparedSingle.map(prepared -> Pair.of(doc, prepared)))

        // buffer some amount of elements, so we can do the document population in parallel
        // since max page size is 20, this means max of 4 buffering
        .buffer(5)
        .flatMap(
            all -> {
              // map to many flowables of RawDocument
              List<Flowable<RawDocument>> flowables =
                  all.stream()
                      .map(
                          p -> {
                            // bind for this doc id
                            RawDocument document = p.getLeft();
                            BoundQuery query = p.getRight().bind(document.id());

                            // fetch, take one and then populate into the original doc
                            // since we have the doc id, use the max storage page size to grab all
                            // the rows
                            // for
                            // that doc
                            return queryExecutor
                                .queryDocs(
                                    query,
                                    configuration.getMaxStoragePageSize(),
                                    false,
                                    null,
                                    context)
                                .firstElement()
                                .map(document::populateFrom)
                                .toFlowable();
                          })
                      .collect(Collectors.toList());

              // this is the trick to execute the all 5 in parallel
              // once they are all done, just map back to flowable
              // note that this respects the order, so we always return in correct order
              return Flowable.zip(
                      flowables, objects -> Arrays.stream(objects).map(RawDocument.class::cast))
                  .flatMap(Flowable::fromStream);
            });
  }

  private ExecutionContext nestedPopulate(ExecutionContext context) {
    return context.nested("LoadProperties");
  }

  private ExecutionContext nestedFullSearch(ExecutionContext context) {
    return context.nested("LoadAllDocuments");
  }

  private ExecutionContext nestedFullDocument(ExecutionContext context) {
    return context.nested("GetFullDocument");
  }
}
