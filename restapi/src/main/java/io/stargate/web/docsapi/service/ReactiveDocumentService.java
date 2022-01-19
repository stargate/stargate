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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.core.util.TimeSource;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.BuiltInApiFunction;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.models.dto.ExecuteBuiltInFunction;
import io.stargate.web.docsapi.rx.RxUtils;
import io.stargate.web.docsapi.service.json.DeadLeafCollector;
import io.stargate.web.docsapi.service.json.DeadLeafCollectorImpl;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeafCollector;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import io.stargate.web.docsapi.service.write.DocumentWriteService;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.PathSegment;
import org.javatuples.Pair;
import org.jsfr.json.JsonSurferJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReactiveDocumentService {

  private static final Logger logger = LoggerFactory.getLogger(ReactiveDocumentService.class);

  @Inject ExpressionParser expressionParser;
  @Inject DocumentSearchService searchService;
  @Inject DocumentWriteService writeService;
  @Inject JsonConverter jsonConverter;
  @Inject JsonSchemaHandler jsonSchemaHandler;
  @Inject DocsShredder docsShredder;
  @Inject JsonDocumentShredder jsonDocumentShredder;
  @Inject ObjectMapper objectMapper;
  @Inject TimeSource timeSource;
  @Inject DocsApiConfiguration configuration;

  public ReactiveDocumentService() {}

  public ReactiveDocumentService(
      ExpressionParser expressionParser,
      DocumentSearchService searchService,
      DocumentWriteService writeService,
      JsonConverter jsonConverter,
      JsonSchemaHandler jsonSchemaHandler,
      DocsShredder docsShredder,
      JsonDocumentShredder jsonDocumentShredder,
      ObjectMapper objectMapper,
      TimeSource timeSource,
      DocsApiConfiguration configuration) {
    this.expressionParser = expressionParser;
    this.searchService = searchService;
    this.writeService = writeService;
    this.jsonConverter = jsonConverter;
    this.jsonSchemaHandler = jsonSchemaHandler;
    this.docsShredder = docsShredder;
    this.jsonDocumentShredder = jsonDocumentShredder;
    this.objectMapper = objectMapper;
    this.timeSource = timeSource;
    this.configuration = configuration;
  }

  /**
   * Writes a document in the given namespace and collection using the randomly generated ID.
   *
   * @param db {@link DocumentDB} to write in
   * @param namespace Namespace
   * @param collection Collection name
   * @param payload Document represented as JSON string
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Single<DocumentResponseWrapper<Void>> writeDocument(
      DocumentDB db,
      String namespace,
      String collection,
      String payload,
      ExecutionContext context) {
    // generate the document id
    String documentId = UUID.randomUUID().toString();

    return Single.defer(
            () -> {

              // authentication for writing before anything
              // we don't need the DELETE scope here
              authorizeWrite(db, namespace, collection, Scope.MODIFY);

              // read the root
              JsonNode root = readPayload(payload);

              // check the schema
              checkSchemaFullDocument(db, namespace, collection, root);

              // shred rows
              List<JsonShreddedRow> rows =
                  jsonDocumentShredder.shred(root, Collections.emptyList());

              // call write document
              return writeService.writeDocument(
                  db.getQueryExecutor().getDataStore(),
                  namespace,
                  collection,
                  documentId,
                  rows,
                  db.treatBooleansAsNumeric(),
                  context);
            })
        .map(any -> new DocumentResponseWrapper<>(documentId, null, null, context.toProfile()));
  }

  /**
   * Updates a document with given ID in the given namespace and collection. Any previously existing
   * document with the same ID will be overwritten.
   *
   * @param db {@link DocumentDB} to write in
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param payload Document represented as JSON string
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Single<DocumentResponseWrapper<Void>> updateDocument(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      String payload,
      ExecutionContext context) {
    List<String> subPath = Collections.emptyList();
    return updateDocument(db, namespace, collection, documentId, subPath, payload, context);
  }

  /**
   * Updates a document with given ID in the given namespace and collection. Any previously existing
   * document with the same ID will be overwritten.
   *
   * @param db {@link DocumentDB} to write in
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param payload Document represented as JSON string
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Single<DocumentResponseWrapper<Void>> updateDocument(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      String payload,
      ExecutionContext context) {

    return Single.defer(
            () -> {
              // authentication for writing before anything
              authorizeWrite(db, namespace, collection, Scope.MODIFY, Scope.DELETE);

              // pre-process to support array elements
              List<String> subPathProcessed = processSubDocumentPath(subPath);

              // read the root
              JsonNode root = readPayload(payload);

              // check the schema
              checkSchemaFullDocument(db, namespace, collection, root);

              // shred rows
              List<JsonShreddedRow> rows = jsonDocumentShredder.shred(root, subPathProcessed);

              // call write document
              return writeService.updateDocument(
                  db.getQueryExecutor().getDataStore(),
                  namespace,
                  collection,
                  documentId,
                  subPathProcessed,
                  rows,
                  db.treatBooleansAsNumeric(),
                  context);
            })
        .map(any -> new DocumentResponseWrapper<>(documentId, null, null, context.toProfile()));
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
          authorizeRead(db, namespace, collection);

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
          authorizeRead(db, namespace, collection);

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
                                      DocsApiUtils.extractArrayPathIndex(
                                              p, configuration.getMaxArrayLength())
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
          authorizeRead(db, namespace, collection);

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

  /**
   * Executes a function against the given document and path, returning a different result based on
   * the ExecuteBuiltInFunction's operation type.
   *
   * <p>Currently supports the functions $push and $pop, which write to the end of or remove a value
   * from the end of an array, respectively.
   *
   * @param db @link DocumentDB} to execute the function in
   * @param keyspace the keyspace to execute the function against
   * @param collection the collection to execute the function against
   * @param id the document ID to execute the function against
   * @param funcPayload information about the function to execute
   * @param path the path within the document
   * @param context execution context
   * @return Maybe containing DocumentResponseWrapper with result node of the function
   */
  public Maybe<DocumentResponseWrapper<? extends JsonNode>> executeBuiltInFunction(
      DocumentDB db,
      String keyspace,
      String collection,
      String id,
      ExecuteBuiltInFunction funcPayload,
      List<PathSegment> path,
      ExecutionContext context) {
    return Maybe.defer(
        () -> {
          List<String> pathString =
              path.stream().map(seg -> seg.getPath()).collect(Collectors.toList());
          Maybe<JsonNode> result = null;
          BuiltInApiFunction function = BuiltInApiFunction.fromName(funcPayload.getOperation());
          if (function == BuiltInApiFunction.ARRAY_PUSH) {
            result =
                handlePush(
                    db, keyspace, collection, id, funcPayload.getValue(), pathString, context);
          } else if (function == BuiltInApiFunction.ARRAY_POP) {
            result = handlePop(db, keyspace, collection, id, pathString, context);
          }
          if (result == null) {
            throw new IllegalStateException(
                "Invalid operation found at execution time: " + function.name);
          }
          return result.map(
              json -> new DocumentResponseWrapper<>(id, null, json, context.toProfile()));
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
        fieldPaths =
            DocsApiUtils.convertFieldsToPaths(fieldsNode, configuration.getMaxArrayLength());
      } catch (JsonProcessingException ex) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FIELDS_JSON_INVALID);
      }
    }
    return fieldPaths;
  }

  // we need to transform the stuff to support array elements
  private List<String> processSubDocumentPath(List<String> subDocumentPath) {
    return subDocumentPath.stream()
        .map(path -> DocsApiUtils.convertArrayPath(path, configuration.getMaxArrayLength()))
        .collect(Collectors.toList());
  }

  /**
   * Retrieves the specified path in the document as an array, appends the @param value, and returns
   * what the array will look like after the append. Note that this doesn't write the array back to
   * the data store.
   *
   * @param db
   * @param namespace
   * @param collection
   * @param documentId
   * @param processedPath
   * @param value
   * @param context
   * @return a JSON representation of the array after pushing the new value
   */
  private Maybe<JsonNode> getArrayAfterPush(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      List<String> processedPath,
      Object value,
      ExecutionContext context) {
    return getDocument(db, namespace, collection, documentId, processedPath, null, context)
        .map(
            array -> {
              JsonNode data = array.getData();
              if (!data.isArray()) {
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID,
                    "The path provided to push to has no array");
              }
              ArrayNode arrayData = (ArrayNode) data;
              arrayData.insertPOJO(arrayData.size(), value);
              return arrayData;
            });
  }

  /**
   * Retrieves the specified path in the document as an array, pops the last (i.e. highest-index)
   * value, and returns both what the array will look like after the pop and the value that was
   * popped. Note that this doesn't write the array back to the data store.
   *
   * @param db
   * @param namespace
   * @param collection
   * @param documentId
   * @param processedPath
   * @param context
   * @return a Pair containing the JSON representation of the array after popping the value and the
   *     value itself
   */
  private Maybe<Pair<ArrayNode, JsonNode>> getArrayAndValueAfterPop(
      DocumentDB db,
      String namespace,
      String collection,
      String documentId,
      List<String> processedPath,
      ExecutionContext context) {
    return getDocument(db, namespace, collection, documentId, processedPath, null, context)
        .map(
            array -> {
              JsonNode data = array.getData();
              if (data == null || !data.isArray()) {
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID,
                    "The path provided to pop from has no array");
              }
              ArrayNode arrayData = (ArrayNode) data;
              if (arrayData.size() == 0) {
                throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_ARRAY_POP_OUT_OF_BOUNDS);
              }
              JsonNode value = arrayData.remove(arrayData.size() - 1);
              return Pair.with(arrayData, value);
            });
  }

  private void writeNewArrayState(
      DocumentDB db,
      String keyspace,
      String collection,
      String id,
      JsonNode jsonArray,
      List<String> pathString,
      ExecutionContext context)
      throws UnauthorizedException {
    List<String> processedPath = processSubDocumentPath(pathString);

    // TODO use here the new shredder and move to the new writer once we implement the write with
    // sub-path

    List<Object[]> bindParams =
        docsShredder.shredPayload(
                JsonSurferJackson.INSTANCE,
                processedPath,
                id,
                jsonArray.toString(),
                false,
                db.treatBooleansAsNumeric(),
                true)
            .left;
    db.deleteThenInsertBatch(
        keyspace,
        collection,
        id,
        bindParams,
        processedPath,
        timeSource.currentTimeMicros(),
        context.nested("ASYNC INSERT"));
  }

  private Maybe<JsonNode> handlePush(
      DocumentDB db,
      String keyspace,
      String collection,
      String id,
      Object valueToPush,
      List<String> pathString,
      ExecutionContext context) {
    return getArrayAfterPush(db, keyspace, collection, id, pathString, valueToPush, context)
        .map(
            jsonArray -> {
              writeNewArrayState(db, keyspace, collection, id, jsonArray, pathString, context);
              return jsonArray;
            });
  }

  private Maybe<JsonNode> handlePop(
      DocumentDB db,
      String keyspace,
      String collection,
      String id,
      List<String> pathString,
      ExecutionContext context) {
    return getArrayAndValueAfterPop(db, keyspace, collection, id, pathString, context)
        .map(
            arrayAndValue -> {
              writeNewArrayState(
                  db, keyspace, collection, id, arrayAndValue.getValue0(), pathString, context);
              return arrayAndValue.getValue1();
            });
  }

  /////////////////////
  // Object helpers  //
  /////////////////////

  private ObjectNode createJsonMap(
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

  private ArrayNode createJsonArray(
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

  private JsonNode documentToNode(
      RawDocument doc,
      Collection<List<String>> fieldPaths,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    ImmutableDeadLeafCollector collector = ImmutableDeadLeafCollector.of();
    return documentToNode(doc, fieldPaths, collector, writeAllPathsAsObjects, numericBooleans);
  }

  private JsonNode documentToNode(
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

  /////////////////////
  /// Write helpers ///
  /////////////////////

  // checks that node is in alignment with schema if exists
  private void checkSchemaFullDocument(
      DocumentDB db, String namespace, String collection, JsonNode root) {
    JsonNode schema = jsonSchemaHandler.getCachedJsonSchema(db, namespace, collection);
    if (null != schema) {
      try {
        jsonSchemaHandler.validate(schema, root);
      } catch (ProcessingException e) {
        // TODO validate unchecked better?
        throw new RuntimeException(e);
      }
    }
  }

  // reads JSON payload
  private JsonNode readPayload(String payload) throws JsonProcessingException {
    try {
      return objectMapper.readTree(payload);
    } catch (JsonProcessingException e) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Malformed JSON object found during read.", e);
    }
  }

  /////////////////////
  ///  Auth helpers ///
  /////////////////////

  // authorizes read
  private void authorizeRead(DocumentDB db, String namespace, String collection)
      throws UnauthorizedException {
    AuthorizationService authorizationService = db.getAuthorizationService();
    AuthenticationSubject authenticationSubject = db.getAuthenticationSubject();
    authorizationService.authorizeDataRead(
        authenticationSubject, namespace, collection, SourceAPI.REST);
  }

  // authorizes write on the given scopes
  private void authorizeWrite(DocumentDB db, String namespace, String collection, Scope... scopes)
      throws UnauthorizedException {
    AuthorizationService authorizationService = db.getAuthorizationService();
    AuthenticationSubject authenticationSubject = db.getAuthenticationSubject();
    for (Scope scope : scopes) {
      authorizationService.authorizeDataWrite(
          authenticationSubject, namespace, collection, scope, SourceAPI.REST);
    }
  }
}
