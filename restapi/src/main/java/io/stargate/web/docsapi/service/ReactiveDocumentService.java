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

package io.stargate.web.docsapi.service;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.service.json.DeadLeafCollector;
import io.stargate.web.docsapi.service.json.DeadLeafCollectorImpl;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeafCollector;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import io.stargate.web.rx.RxUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReactiveDocumentService {

  private static final Logger logger = LoggerFactory.getLogger(ReactiveDocumentService.class);

  @Inject ExpressionParser expressionParser;
  @Inject DocumentSearchService searchService;
  @Inject JsonConverter jsonConverter;
  @Inject ObjectMapper objectMapper;
  @Inject TimeSource timeSource;

  public ReactiveDocumentService() {}

  public ReactiveDocumentService(
      ExpressionParser expressionParser,
      DocumentSearchService searchService,
      JsonConverter jsonConverter,
      ObjectMapper objectMapper,
      TimeSource timeSource) {
    this.expressionParser = expressionParser;
    this.searchService = searchService;
    this.jsonConverter = jsonConverter;
    this.objectMapper = objectMapper;
    this.timeSource = timeSource;
  }

  /**
   * Searches for documents in the whole collection.
   *
   * @param db {@link DocumentDB} to search in
   * @param namespace Namespace
   * @param collection Collection name
   * @param where Conditions
   * @param fields Fields to include in returned documents
   * @param paginator Paginator
   * @param context Execution content
   * @return Single containing DocumentResponseWrapper, in case no results found it will contain an
   *     empty json node
   */
  public Single<DocumentResponseWrapper<? extends JsonNode>> findDocuments(
      DocumentDB db,
      String namespace,
      String collection,
      String where,
      String fields,
      Paginator paginator,
      ExecutionContext context) {

    // everything in the reactive sequence
    return Single.defer(
        () -> {
          // resolve the inputs first
          Expression<FilterExpression> expression =
              getExpression(db, Collections.emptyList(), where);
          Collection<List<String>> fieldPaths = getFields(fields);

          // authentication for the read before searching
          AuthorizationService authorizationService = db.getAuthorizationService();
          AuthenticationSubject authenticationSubject = db.getAuthenticationSubject();
          authorizationService.authorizeDataRead(
              authenticationSubject, namespace, collection, SourceAPI.REST);

          // call the search service
          return searchService
              .searchDocuments(
                  db.getQueryExecutor(), namespace, collection, expression, paginator, context)

              // collect and make sure it's not empty
              .toList()
              .filter(rawDocuments -> !rawDocuments.isEmpty())

              // map to the json & ensure page state is updated in the wrapped
              .map(
                  rawDocuments -> {
                    String state = Paginator.makeExternalPagingState(paginator, rawDocuments);

                    ObjectNode docsResult = createJsonMap(db, rawDocuments, fieldPaths, false);
                    return new DocumentResponseWrapper<JsonNode>(
                        null, state, docsResult, context.toProfile());
                  })
              .switchIfEmpty(
                  Single.fromSupplier(
                      () -> {
                        ObjectNode emptyNode = objectMapper.createObjectNode();
                        return new DocumentResponseWrapper<>(
                            null, null, emptyNode, context.toProfile());
                      }));
        });
  }

  /**
   * Gets all sub-documents of a single document at the given path, or a complete document if
   * #subDocumentPath is empty. Response structure is key to value pairs, where key matches the key
   * at the given #subDocumentPath.
   *
   * <p><b>Note:</b> this method does use the {@link
   * io.stargate.web.docsapi.service.json.DeadLeafCollectorImpl} and as a side effect deletes the
   * dead leaves.
   *
   * <p><b>Note:</b> this method does not accept {@link Paginator} and fetches results in a way that
   * everything is returned. If you need pagination, use #searchSubDocuments.
   *
   * @param db {@link DocumentDB} to search in
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId Document to get
   * @param subDocumentPath path to look for sub documents (empty means get complete doc)
   * @param fields Fields to include in returned documents
   * @param context Execution content
   * @return Maybe containing DocumentResponseWrapper with result node and no paging state
   */
  public Maybe<DocumentResponseWrapper<? extends JsonNode>> getDocument(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      String fields,
      ExecutionContext context) {
    return getDocumentInternal(
            db, namespace, collection, documentId, subDocumentPath, fields, context)
        .map(Pair::getValue0);
  }

  /**
   * See {@link #getDocument(DocumentDB, String, String, String, List, String, ExecutionContext)}
   *
   * @return a Maybe pair of the {@link DocumentResponseWrapper}, and a {@link Disposable} for a
   *     potentially issued "dead leaf" deletion batch.
   */
  @VisibleForTesting
  Maybe<Pair<DocumentResponseWrapper<? extends JsonNode>, Disposable>> getDocumentInternal(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      String fields,
      ExecutionContext context) {

    long now = timeSource.currentTimeMicros();

    // everything in the reactive sequence
    return Maybe.defer(
        () -> {
          // resolve the inputs first
          Collection<List<String>> fieldPaths = getFields(fields);

          // authentication for the read before searching
          AuthorizationService authorizationService = db.getAuthorizationService();
          AuthenticationSubject authenticationSubject = db.getAuthenticationSubject();
          authorizationService.authorizeDataRead(
              authenticationSubject, namespace, collection, SourceAPI.REST);

          // we need to check that we have no globs and transform the stuff to support array
          // elements
          List<String> subDocumentPathProcessed = processSubDocumentPath(subDocumentPath);

          // backward compatibility
          // fields relative to the sub-path
          Collection<List<String>> fieldPathsFinal =
              fieldPaths.stream()
                  .peek(l -> l.addAll(0, subDocumentPathProcessed))
                  .collect(Collectors.toList());

          // call the search service
          return searchService
              .getDocument(
                  db.getQueryExecutor(),
                  namespace,
                  collection,
                  documentId,
                  subDocumentPathProcessed,
                  context)

              // one document only
              .singleElement()

              // map to the json
              .flatMap(
                  document -> {
                    DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
                    JsonNode docsResult =
                        documentToNode(
                            document,
                            fieldPathsFinal,
                            collector,
                            false,
                            db.treatBooleansAsNumeric());

                    Disposable deleteBatch;
                    // dead leaf deletion init on non-empty collection
                    if (!collector.isEmpty()) {
                      int size = collector.getLeaves().size();
                      // Submit the DELETE batch for async execution (do not block, do not wait)
                      // Note: authorizeDeleteDeadLeaves is called only if dead leaves are found.
                      deleteBatch =
                          Single.fromCallable(
                                  () -> db.authorizeDeleteDeadLeaves(namespace, collection))
                              .filter(
                                  authorized -> {
                                    // Don't fail this read request if the corrective DELETE
                                    // statements
                                    // are not authorized, simply skip DELETE batch in that case.
                                    if (authorized) {
                                      logger.info("Deleting {} dead leaves", size);
                                    } else {
                                      logger.info("Not authorized to delete {} dead leaves", size);
                                    }

                                    return authorized;
                                  })
                              .flatMap(
                                  __ ->
                                      RxUtils.singleFromFuture(
                                              () ->
                                                  db.deleteDeadLeaves(
                                                      namespace,
                                                      collection,
                                                      documentId,
                                                      now,
                                                      collector.getLeaves(),
                                                      context))
                                          .toMaybe())
                              .subscribeOn(Schedulers.io())
                              .doOnSuccess(__ -> logger.info("Deleted {} dead leaves", size))
                              .doOnError(t -> logger.error("Unable to delete dead leaves: " + t, t))
                              .subscribe();
                    } else {
                      deleteBatch = Disposable.disposed();
                    }

                    // create json pattern expression if sub path is defined
                    if (!subDocumentPath.isEmpty()) {
                      String jsonPtrExpr =
                          subDocumentPath.stream()
                              .map(
                                  p ->
                                      DocsApiUtils.extractArrayPathIndex(p)
                                          .map(Object::toString)
                                          .orElse(DocsApiUtils.convertEscapedCharacters(p)))
                              .collect(Collectors.joining("/", "/", ""));

                      // find and return empty if missing
                      docsResult = docsResult.at(jsonPtrExpr);
                      if (docsResult.isMissingNode()) {
                        return Maybe.empty();
                      }
                    }

                    DocumentResponseWrapper<JsonNode> wrapper =
                        new DocumentResponseWrapper<>(
                            documentId, null, docsResult, context.toProfile());
                    return Maybe.just(Pair.with(wrapper, deleteBatch));
                  });
        });
  }

  /**
   * Searches for all sub-documents of a single document at the given path, or a complete document
   * if #subDocumentPath is empty. Response structure is an array with full found sub-doc structure.
   *
   * <p><b>Note:</b> this method has no side effects
   *
   * @param db {@link DocumentDB} to search in
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId Document to get
   * @param subDocumentPath path to look for sub documents (empty means search complete doc)
   * @param where Conditions to be matches at the #subDocumentPath (max 1 filter path)
   * @param fields Fields to include in returned documents (relative to the one filter path)
   * @param paginator for defining page size
   * @param context Execution content
   * @return Maybe containing DocumentResponseWrapper with result node and paging state
   * @throws ErrorCodeRuntimeException If more then one filter path is supplied and if fields are
   *     not containing the filter path field
   */
  public Maybe<DocumentResponseWrapper<? extends JsonNode>> findSubDocuments(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      String where,
      String fields,
      Paginator paginator,
      ExecutionContext context) {

    // everything in the reactive sequence
    return Maybe.defer(
        () -> {
          // resolve the inputs first
          Expression<FilterExpression> expression = getExpression(db, subDocumentPath, where);
          Collection<List<String>> fieldPaths = getFields(fields);

          // backward compatibility checks
          Set<FilterExpression> expressionSet = new HashSet<>();
          expression.collectK(expressionSet, Integer.MAX_VALUE);
          List<FilterPath> filterPaths =
              expressionSet.stream()
                  .map(FilterExpression::getFilterPath)
                  .distinct()
                  .collect(Collectors.toList());

          // only single filter path
          if (filterPaths.size() > 1) {
            String msg =
                String.format(
                    "Conditions across multiple fields are not yet supported. Found: %d.",
                    fieldPaths.size());
            throw new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_GET_MULTIPLE_FIELD_CONDITIONS, msg);
          }

          FilterPath filterPath = filterPaths.isEmpty() ? null : filterPaths.get(0);
          // field of condition must be referenced in the fields (if they exist)
          if (!fieldPaths.isEmpty()
              && filterPath != null
              && !fieldPaths.contains(Collections.singletonList(filterPath.getField()))) {
            throw new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_GET_CONDITION_FIELDS_NOT_REFERENCED);
          }

          // authentication for the read before searching
          AuthorizationService authorizationService = db.getAuthorizationService();
          AuthenticationSubject authenticationSubject = db.getAuthenticationSubject();
          authorizationService.authorizeDataRead(
              authenticationSubject, namespace, collection, SourceAPI.REST);

          // we need to check that we have no globs and transform the stuff to support array
          // elements
          List<String> subDocumentPathProcessed = processSubDocumentPath(subDocumentPath);

          // yet another backward compatibility fix
          // fields are relative to that single filter parent path if it exists
          // otherwise to the path prefix
          Collection<List<String>> finalFieldPath =
              getFinalInDocumentFieldPaths(fieldPaths, filterPath, subDocumentPathProcessed);

          // final search sub-path is either the given one or the filter parent path if exists
          List<String> searchPath =
              null != filterPath ? filterPath.getParentPath() : subDocumentPathProcessed;

          // call the search service
          return searchService
              .searchSubDocuments(
                  db.getQueryExecutor(),
                  namespace,
                  collection,
                  documentId,
                  searchPath,
                  expression,
                  paginator,
                  context)

              // collect and make sure it's not empty
              .toList()
              .filter(rawDocuments -> !rawDocuments.isEmpty())

              // map to the json & ensure page state is updated in the wrapped
              .map(
                  rawDocuments -> {
                    String state = Paginator.makeExternalPagingState(paginator, rawDocuments);

                    // NOTE: Search writes all paths as objects
                    ArrayNode docsResult = createJsonArray(db, rawDocuments, finalFieldPath, true);
                    return new DocumentResponseWrapper<JsonNode>(
                        documentId, state, docsResult, context.toProfile());
                  });
        });
  }

  private Collection<List<String>> getFinalInDocumentFieldPaths(
      Collection<List<String>> fieldPaths,
      FilterPath filterPath,
      List<String> subDocumentPathProcessed) {
    // fields are relative to that single filter parent path if it exists
    // otherwise to the path prefix
    // execute two different cases
    if (null != filterPath) {
      return Optional.of(fieldPaths)
          .filter(fp -> !fp.isEmpty())
          .map(
              fp ->
                  fp.stream()
                      .peek(l -> l.addAll(0, filterPath.getParentPath()))
                      .collect(Collectors.toList()))
          .orElse(Collections.singletonList(filterPath.getPath()));

    } else {
      return Optional.of(fieldPaths)
          .filter(fp -> !fp.isEmpty())
          .map(
              fp ->
                  fp.stream()
                      .peek(l -> l.addAll(0, subDocumentPathProcessed))
                      .collect(Collectors.toList()))
          .orElse(Collections.singletonList(subDocumentPathProcessed));
    }
  }

  private Expression<FilterExpression> getExpression(
      DocumentDB db, List<String> prependPath, String where) {
    Expression<FilterExpression> expression = Literal.getTrue();
    if (null != where) {
      try {
        JsonNode whereNode = objectMapper.readTree(where);
        expression =
            expressionParser.constructFilterExpression(
                prependPath, whereNode, db.treatBooleansAsNumeric());
      } catch (JsonProcessingException ex) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_WHERE_JSON_INVALID);
      }
    }
    return expression;
  }

  private Collection<List<String>> getFields(String fields) {
    Collection<List<String>> fieldPaths = Collections.emptyList();
    if (null != fields) {
      try {
        JsonNode fieldsNode = objectMapper.readTree(fields);
        fieldPaths = DocsApiUtils.convertFieldsToPaths(fieldsNode);
      } catch (JsonProcessingException ex) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FIELDS_JSON_INVALID);
      }
    }
    return fieldPaths;
  }

  // we need to transform the stuff to support array elements
  private List<String> processSubDocumentPath(List<String> subDocumentPath) {
    return subDocumentPath.stream()
        .map(path -> DocsApiUtils.convertArrayPath(path))
        .collect(Collectors.toList());
  }

  public ObjectNode createJsonMap(
      DocumentDB db,
      List<RawDocument> docs,
      Collection<List<String>> fieldPaths,
      boolean writeAllPathsAsObjects) {
    ObjectNode docsResult = objectMapper.createObjectNode();

    for (RawDocument doc : docs) {
      // create document node and set to result
      JsonNode node =
          documentToNode(doc, fieldPaths, writeAllPathsAsObjects, db.treatBooleansAsNumeric());
      docsResult.set(doc.id(), node);
    }

    return docsResult;
  }

  public ArrayNode createJsonArray(
      DocumentDB db,
      List<RawDocument> docs,
      Collection<List<String>> fieldPaths,
      boolean writeAllPathsAsObjects) {
    ArrayNode docsResult = objectMapper.createArrayNode();

    for (RawDocument doc : docs) {
      // create document node and set to result
      JsonNode node =
          documentToNode(doc, fieldPaths, writeAllPathsAsObjects, db.treatBooleansAsNumeric());

      // skip adding empty nodes to the results array
      if (!node.isEmpty()) {
        docsResult.add(node);
      }
    }

    return docsResult;
  }

  public JsonNode documentToNode(
      RawDocument doc,
      Collection<List<String>> fieldPaths,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    ImmutableDeadLeafCollector collector = ImmutableDeadLeafCollector.of();
    return documentToNode(doc, fieldPaths, collector, writeAllPathsAsObjects, numericBooleans);
  }

  public JsonNode documentToNode(
      RawDocument doc,
      Collection<List<String>> fieldPaths,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    // filter needed rows only
    List<Row> rows = doc.rows();
    if (!fieldPaths.isEmpty()) {
      rows =
          doc.rows().stream()
              .filter(
                  row ->
                      fieldPaths.stream()
                          .anyMatch(fieldPath -> DocsApiUtils.isRowOnPath(row, fieldPath)))
              .collect(Collectors.toList());
    }

    // create document node and set to result
    return jsonConverter.convertToJsonDoc(rows, collector, writeAllPathsAsObjects, numericBooleans);
  }
}
