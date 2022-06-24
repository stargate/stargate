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

package io.stargate.sgv2.docsapi.service.query;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import io.opentelemetry.extension.annotations.WithSpan;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.query.executor.QueryExecutor;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.DocumentTtlQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.FullSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.PopulateSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.db.impl.SubDocumentSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.query.search.resolver.BaseResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.DocumentsResolver;
import io.stargate.sgv2.docsapi.service.query.search.resolver.impl.SubDocumentsResolver;
import java.util.List;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

@ApplicationScoped
public class ReadBridgeService {

  @Inject QueryExecutor queryExecutor;

  @Inject DocumentProperties documentProperties;

  /**
   * Searches a complete collection in order to find the documents that match the given expression.
   * Starts the search for the given {@link Paginator} state.
   *
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param expression Expression tree
   * @param paginator {@link Paginator}
   * @param context Context for recording profiling information
   * @return Multi of {@link RawDocument}s, limited to a size defined in the #paginator.
   */
  @WithSpan
  public Multi<RawDocument> searchDocuments(
      String keyspace,
      String collection,
      Expression<FilterExpression> expression,
      Paginator paginator,
      ExecutionContext context) {

    // if we have true immediately, means we can only do full search
    if (Literal.EXPR_TYPE.equals(expression.getExprType())) {

      // for the sake of correctness make sure we don't have a false
      if (Literal.getFalse().equals(expression)) {
        return Multi.createFrom().empty();
      }

      // fetch docs, no need to populate as all rows are taken
      return fullSearch(keyspace, collection, paginator, nestedFullSearch(context))

          // load only for the page size
          .select()
          .first(paginator.docPageSize);
    } else {
      // otherwise resolve the expression
      DocumentsResolver documentsResolver =
          BaseResolver.resolve(expression, context, documentProperties);

      // load the candidates
      Multi<RawDocument> candidates =
          documentsResolver
              .getDocuments(queryExecutor, keyspace, collection, paginator)

              // limit to requested page size only to stop fetching extra docs
              .select()
              .first(paginator.docPageSize);

      // then populate
      return populateCandidates(candidates, keyspace, collection, nestedPopulate(context));
    }
  }

  /**
   * Searches a single document in order to find the sub-documents that match the given expression.
   * Sub-documents are defined by the #subDocumentPath, everything outside this path is ignored.
   * Starts the search for the given {@link Paginator} state.
   *
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param documentId Document ID to search in
   * @param subDocumentPath Path where to find sub-documents
   * @param expression Expression tree to fulfill (note that #subDocumentPath must be already
   *     included in the {@link FilterExpression}s)
   * @param paginator {@link Paginator}
   * @param context Context for recording profiling information
   * @return Multi of {@link RawDocument}s representing sub-documents in the given #subDocumentPath,
   *     limited to a size defined in the #paginator
   */
  @WithSpan
  public Multi<RawDocument> searchSubDocuments(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      Expression<FilterExpression> expression,
      Paginator paginator,
      ExecutionContext context) {

    // for the sake of correctness make sure we don't have false
    if (Literal.getFalse().equals(expression)) {
      return Multi.createFrom().empty();
    }

    // create the resolver and return results
    SubDocumentsResolver subDocumentsResolver =
        new SubDocumentsResolver(
            expression, documentId, subDocumentPath, context, documentProperties);
    return subDocumentsResolver
        .getDocuments(queryExecutor, keyspace, collection, paginator)

        // limit to requested page size only to stop fetching extra docs
        .select()
        .first(paginator.docPageSize);
  }

  /**
   * Gets a single document optionally limited to the #subDocumentPath.
   *
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param documentId Document ID to search in
   * @param subDocumentPath Path where to find the document
   * @param context Context for recording profiling information
   * @return Multi with a single {@link RawDocument} representing a document or sub-document in the
   *     given #subDocumentPath, or empty if not found.
   */
  @WithSpan
  public Multi<RawDocument> getDocument(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {

    return fullDocument(
            keyspace, collection, documentId, subDocumentPath, nestedFullDocument(context))

        // take one, as there can be only one document
        .select()
        .first();
  }

  /**
   * Gets a single document's rows with its TTL data in each row.
   *
   * @param keyspace Keyspace to search in.
   * @param collection Collection to search in.
   * @param documentId Document ID to search in
   * @param context Context for recording profiling information
   * @return Uni with a single {@link RawDocument} representing a document's rows with TTL as a
   *     column, or null if document not found
   */
  @WithSpan
  public Uni<RawDocument> getDocumentTtlInfo(
      String keyspace, String collection, String documentId, ExecutionContext context) {
    return documentTtl(keyspace, collection, documentId, context).select().first().toUni();
  }

  private Multi<RawDocument> fullSearch(
      String keyspace, String collection, Paginator paginator, ExecutionContext context) {

    // build and bind first (this could be cached for the max depth)
    return Uni.createFrom()
        .item(
            () -> {
              String[] columns = documentProperties.tableColumns().allColumnNamesArray();

              FullSearchQueryBuilder queryBuilder = new FullSearchQueryBuilder(documentProperties);
              QueryOuterClass.Query query = queryBuilder.buildQuery(keyspace, collection, columns);
              return queryBuilder.bind(query);
            })
        .memoize()
        .indefinitely()
        .onItem()
        .transformToMulti(
            query ->
                queryExecutor.queryDocs(
                    query,
                    documentProperties.getApproximateStoragePageSize(paginator.docPageSize),
                    true,
                    paginator.getCurrentDbPageState(),
                    true,
                    context));
  }

  private Multi<RawDocument> documentTtl(
      String keyspace, String collection, String documentId, ExecutionContext context) {

    DocumentTtlQueryBuilder queryBuilder = new DocumentTtlQueryBuilder(documentProperties);

    // prepare first
    return Uni.createFrom()
        .item(() -> queryBuilder.buildQuery(keyspace, collection))
        .memoize()
        .indefinitely()
        .onItem()
        .transformToMulti(
            built -> {
              QueryOuterClass.Value idValue = Values.of(documentId);
              QueryOuterClass.Query query = queryBuilder.bindWithValues(built, idValue);
              return queryExecutor.queryDocs(
                  query, documentProperties.maxSearchPageSize(), false, null, false, context);
            });
  }

  private Multi<RawDocument> fullDocument(
      String keyspace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      ExecutionContext context) {

    // build and bind first
    return Uni.createFrom()
        .item(
            () -> {
              String[] columns = documentProperties.tableColumns().allColumnNamesArray();

              SubDocumentSearchQueryBuilder queryBuilder =
                  new SubDocumentSearchQueryBuilder(
                      documentProperties, documentId, subDocumentPath);
              QueryOuterClass.Query query = queryBuilder.buildQuery(keyspace, collection, columns);
              return queryBuilder.bind(query);
            })
        .memoize()
        .indefinitely()
        .onItem()
        .transformToMulti(
            query -> {
              // note that we fetch row paging, only if key depth is more than 1
              // if it's one, then we are getting a whole document, thus no paging
              int keyDepth = subDocumentPath.size() + 1;
              boolean fetchRowPaging = keyDepth > 1;

              // since we have the doc id, use the max storage page size to grab all the rows for
              // that doc
              return queryExecutor.queryDocs(
                  keyDepth,
                  query,
                  documentProperties.maxSearchPageSize(),
                  false,
                  null,
                  fetchRowPaging,
                  context);
            });
  }

  // populates the given documents by using a prepared query
  private Multi<RawDocument> populateCandidates(
      Multi<RawDocument> candidates, String keyspace, String collection, ExecutionContext context) {

    PopulateSearchQueryBuilder queryBuilder = new PopulateSearchQueryBuilder(documentProperties);

    // prepare query
    Uni<QueryOuterClass.Query> preparedSingle =
        Uni.createFrom()
            .item(
                () -> {
                  String[] columns = documentProperties.tableColumns().allColumnNamesArray();

                  // build
                  return queryBuilder.buildQuery(keyspace, collection, columns);
                })
            // then cache so we can reuse for each document
            .memoize()
            .indefinitely();

    // combine
    return candidates

        // keep the order of incoming docs
        .onItem()
        .transformToUniAndConcatenate(doc -> preparedSingle.map(prepared -> Pair.of(doc, prepared)))

        // buffer some amount of elements, so we can do the document population in parallel
        // since max page size is 20, this means max of 4 buffering
        .group()
        .intoLists()
        .of(5)

        // then concat map to respect order
        .concatMap(
            all -> {
              // map to list of single item multi of RawDocument
              List<Multi<RawDocument>> multiList =
                  all.stream()
                      .map(
                          p -> {
                            // bind for this doc id
                            RawDocument document = p.getLeft();
                            QueryOuterClass.Value idValue = Values.of(document.id());
                            QueryOuterClass.Query query =
                                queryBuilder.bindWithValues(p.getRight(), idValue);

                            // fetch, take one and then populate into the original doc
                            // since we have the doc id, use the max storage page size to grab all
                            // the rows
                            // for
                            // that doc
                            return queryExecutor
                                .queryDocs(
                                    query,
                                    documentProperties.maxSearchPageSize(),
                                    false,
                                    null,
                                    false,
                                    context)
                                .select()
                                .first()
                                .map(document::populateFrom);
                          })
                      .collect(Collectors.toList());

              // this is the trick to execute the all 5 in parallel
              // once they are all done, just map back to flowable
              // note that this respects the order, so we always return in correct order
              return Multi.createBy().concatenating().streams(multiList);
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
