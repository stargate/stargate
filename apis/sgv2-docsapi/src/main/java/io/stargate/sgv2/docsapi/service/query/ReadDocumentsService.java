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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Metadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.GrpcMetadataResolver;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.api.v2.model.dto.ExecutionProfile;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.common.model.Paginator;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.json.DeadLeafCollector;
import io.stargate.sgv2.docsapi.service.json.DeadLeafCollectorImpl;
import io.stargate.sgv2.docsapi.service.json.JsonConverter;
import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import io.stargate.sgv2.docsapi.service.util.TimeSource;
import io.stargate.sgv2.docsapi.service.write.WriteBridgeService;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ReadDocumentsService {

  private static final Logger logger = LoggerFactory.getLogger(ReadDocumentsService.class);

  @Inject StargateRequestInfo requestInfo;

  @Inject GrpcMetadataResolver metadataResolver;

  @Inject ReadBridgeService readBridgeService;

  @Inject WriteBridgeService writeBridgeService;

  @Inject ExpressionParser expressionParser;

  @Inject JsonConverter jsonConverter;

  @Inject DocumentProperties documentProperties;

  @Inject DataStoreProperties dataStoreProperties;

  @Inject TimeSource timeSource;

  @Inject ObjectMapper objectMapper;

  /**
   * Searches for documents in the whole collection.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param where Conditions
   * @param fields Fields to include in returned documents, must be a JSON array
   * @param paginator Paginator
   * @param context Execution content
   * @return Uni containing DocumentResponseWrapper, in case no results found it will contain an
   *     empty json node
   */
  public Uni<DocumentResponseWrapper<JsonNode>> findDocuments(
      String namespace,
      String collection,
      String where,
      String fields,
      Paginator paginator,
      ExecutionContext context) {

    // everything in the reactive sequence
    return Uni.createFrom()
        .deferred(
            () -> {

              // resolve the inputs first
              Expression<FilterExpression> expression =
                  getExpression(Collections.emptyList(), where);
              Collection<List<String>> fieldPaths = getFields(fields);

              // call the search service
              return readBridgeService
                  .searchDocuments(namespace, collection, expression, paginator, context)

                  // collect
                  .collect()
                  .asList()

                  // map based on if empty or not
                  .map(
                      rawDocuments -> {
                        // create execution profile
                        ExecutionProfile profile = context.toProfile();

                        // map to the json & ensure page state is updated in the wrapped
                        // if empty, ensure empty node
                        if (!rawDocuments.isEmpty()) {
                          String state = Paginator.makeExternalPagingState(paginator, rawDocuments);

                          ObjectNode docsResult = createJsonMap(rawDocuments, fieldPaths, false);
                          return new DocumentResponseWrapper<>(null, state, docsResult, profile);
                        } else {
                          ObjectNode emptyNode = objectMapper.createObjectNode();
                          return new DocumentResponseWrapper<>(null, null, emptyNode, profile);
                        }
                      });
            });
  }

  /**
   * Gets all sub-documents of a single document at the given path, or a complete document if
   * #subDocumentPath is empty. Response structure is key to value pairs, where key matches the key
   * at the given #subDocumentPath.
   *
   * <p><b>Note:</b> this method does use the {@link DeadLeafCollectorImpl} and as a side effect
   * deletes the dead leaves.
   *
   * <p><b>Note:</b> this method does not accept {@link Paginator} and fetches results in a way that
   * everything is returned. If you need pagination, use #searchSubDocuments.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId Document to get
   * @param subDocumentPath path to look for sub documents (empty means get complete doc)
   * @param fields Fields to include in returned document(s), must be a JSON array
   * @param context Execution content
   * @return Uni emitting DocumentResponseWrapper with result node and no paging state, or emitting
   *     null if the document can not be found
   */
  public Uni<DocumentResponseWrapper<JsonNode>> getDocument(
      String namespace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      String fields,
      ExecutionContext context) {
    return getDocumentInternal(namespace, collection, documentId, subDocumentPath, fields, context)

        // map only if internal returns something
        .onItem()
        .ifNotNull()
        .transform(Pair::getLeft);
  }

  /**
   * See {@link #getDocument(String, String, String, List, String, ExecutionContext)}
   *
   * @return a Uni pair of the {@link DocumentResponseWrapper}, and a {@link Cancellable} for a
   *     potentially issued "dead leaf" deletion batch.
   */
  @VisibleForTesting
  Uni<Pair<DocumentResponseWrapper<JsonNode>, Cancellable>> getDocumentInternal(
      String namespace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      String fields,
      ExecutionContext context) {

    long now = timeSource.currentTimeMicros();
    Metadata metadata = metadataResolver.getMetadata(requestInfo);

    // everything in the reactive sequence
    return Uni.createFrom()
        .deferred(
            () -> {
              // resolve the inputs first
              Collection<List<String>> fieldPaths = getFields(fields);

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
              return readBridgeService
                  .getDocument(namespace, collection, documentId, subDocumentPathProcessed, context)

                  // one document only
                  .select()
                  .first()

                  // map to the json
                  .flatMap(
                      document -> {
                        DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
                        JsonNode docsResult =
                            documentToNode(document, fieldPathsFinal, collector, false);

                        Cancellable deleteBatch = () -> {};

                        // dead leaf deletion init on non-empty collection
                        if (!collector.isEmpty()) {
                          int size = collector.getLeaves().size();
                          logger.debug("Deleting {} dead leaves", size);

                          // Submit the DELETE batch for async execution (do not block, do not wait)
                          writeBridgeService
                              .deleteDeadLeaves(
                                  namespace,
                                  collection,
                                  documentId,
                                  now,
                                  collector.getLeaves(),
                                  context,
                                  metadata)

                              // subscribe on the worker thread and don't block
                              .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                              .subscribe()
                              .with(
                                  success -> logger.debug("Deleted {} dead leaves", size),
                                  error -> logger.warn("Unable to delete dead leaves.", error));
                        }

                        // create json pattern expression if sub path is defined
                        if (!subDocumentPath.isEmpty()) {
                          String jsonPtrExpr =
                              subDocumentPath.stream()
                                  .map(
                                      p ->
                                          DocsApiUtils.extractArrayPathIndex(
                                                  p, documentProperties.maxArrayLength())
                                              .map(Object::toString)
                                              .orElse(DocsApiUtils.convertEscapedCharacters(p)))
                                  .collect(Collectors.joining("/", "/", ""));

                          // find and return empty if missing
                          docsResult = docsResult.at(jsonPtrExpr);
                          if (docsResult.isMissingNode()) {
                            return Multi.createFrom().empty();
                          }
                        }

                        ExecutionProfile profile = context.toProfile();
                        DocumentResponseWrapper<JsonNode> wrapper =
                            new DocumentResponseWrapper<>(documentId, null, docsResult, profile);
                        return Multi.createFrom().item(Pair.of(wrapper, deleteBatch));
                      })

                  // transform at the end
                  .toUni();
            });
  }

  /**
   * Searches for all sub-documents of a single document at the given path, or a complete document
   * if #subDocumentPath is empty. Response structure is an array with full found sub-doc structure.
   *
   * <p><b>Note:</b> this method has no side effects
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId Document to get
   * @param subDocumentPath path to look for sub documents (empty means search complete doc)
   * @param where Conditions, to be matches at the #subDocumentPath (max 1 filter path)
   * @param fields Fields to include in returned documents, must be a JSON array (relative to the
   *     one filter path)
   * @param paginator for defining page size
   * @param context Execution content
   * @return Uni containing DocumentResponseWrapper with result node and paging state, or null if
   *     document does not exist
   * @throws ErrorCodeRuntimeException If more than one filter path is supplied, and if fields are
   *     not containing the filter path field
   */
  public Uni<DocumentResponseWrapper<JsonNode>> findSubDocuments(
      String namespace,
      String collection,
      String documentId,
      List<String> subDocumentPath,
      String where,
      String fields,
      Paginator paginator,
      ExecutionContext context) {

    // everything in the reactive sequence
    return Uni.createFrom()
        .deferred(
            () -> {
              // resolve the inputs first
              Expression<FilterExpression> expression = getExpression(subDocumentPath, where);
              Collection<List<String>> fieldPaths = getFields(fields);

              // backward compatibility checks
              Set<FilterExpression> expressionSet = new HashSet<>();
              expression.collectK(expressionSet, Integer.MAX_VALUE);
              List<FilterPath> filterPaths =
                  expressionSet.stream().map(FilterExpression::getFilterPath).distinct().toList();

              // only single filter path
              if (filterPaths.size() > 1) {
                String msg =
                    String.format(
                        "Conditions across multiple fields are not yet supported. Found: %d.",
                        filterPaths.size());
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
              return readBridgeService
                  .searchSubDocuments(
                      namespace, collection, documentId, searchPath, expression, paginator, context)

                  // collect and make sure it's not empty
                  .collect()
                  .asList()

                  // map to the json & ensure page state is updated in the wrapped
                  .map(
                      rawDocuments -> {
                        if (rawDocuments.isEmpty()) {
                          return null;
                        }

                        String state = Paginator.makeExternalPagingState(paginator, rawDocuments);

                        // NOTE: Search writes all paths as objects
                        ArrayNode docsResult = createJsonArray(rawDocuments, finalFieldPath, true);
                        ExecutionProfile profile = context.toProfile();
                        return new DocumentResponseWrapper<JsonNode>(
                            documentId, state, docsResult, profile);
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

  private Expression<FilterExpression> getExpression(List<String> prependPath, String where) {
    Expression<FilterExpression> expression = Literal.getTrue();
    if (null != where) {
      try {
        JsonNode whereNode = objectMapper.readTree(where);
        expression = expressionParser.constructFilterExpression(prependPath, whereNode);
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
            DocsApiUtils.convertFieldsToPaths(fieldsNode, documentProperties.maxArrayLength());
      } catch (JsonProcessingException ex) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FIELDS_JSON_INVALID);
      }
    }
    return fieldPaths;
  }

  // we need to transform the stuff to support array elements
  private List<String> processSubDocumentPath(List<String> subDocumentPath) {
    return subDocumentPath.stream()
        .map(path -> DocsApiUtils.convertArrayPath(path, documentProperties.maxArrayLength()))
        .collect(Collectors.toList());
  }

  /////////////////////
  // Object helpers  //
  /////////////////////

  private ObjectNode createJsonMap(
      List<RawDocument> docs, Collection<List<String>> fieldPaths, boolean writeAllPathsAsObjects) {
    ObjectNode docsResult = objectMapper.createObjectNode();

    for (RawDocument doc : docs) {
      // create document node and set to result
      JsonNode node = documentToNode(doc, fieldPaths, writeAllPathsAsObjects);
      docsResult.set(doc.id(), node);
    }

    return docsResult;
  }

  private ArrayNode createJsonArray(
      List<RawDocument> docs, Collection<List<String>> fieldPaths, boolean writeAllPathsAsObjects) {
    ArrayNode docsResult = objectMapper.createArrayNode();

    for (RawDocument doc : docs) {
      // create document node and set to result
      JsonNode node = documentToNode(doc, fieldPaths, writeAllPathsAsObjects);

      // skip adding empty nodes to the results array
      if (!node.isEmpty()) {
        docsResult.add(node);
      }
    }

    return docsResult;
  }

  private JsonNode documentToNode(
      RawDocument doc, Collection<List<String>> fieldPaths, boolean writeAllPathsAsObjects) {
    DeadLeafCollector collector = new DeadLeafCollectorImpl();
    return documentToNode(doc, fieldPaths, collector, writeAllPathsAsObjects);
  }

  private JsonNode documentToNode(
      RawDocument doc,
      Collection<List<String>> fieldPaths,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects) {
    // filter needed rows only
    List<RowWrapper> rows = doc.rows();
    if (!fieldPaths.isEmpty()) {
      rows =
          doc.rows().stream()
              .filter(
                  row ->
                      fieldPaths.stream()
                          .anyMatch(
                              fieldPath ->
                                  DocsApiUtils.isRowOnPath(row, fieldPath, documentProperties)))
              .collect(Collectors.toList());
    }

    // create document node and set to result
    return jsonConverter.convertToJsonDoc(
        rows, collector, writeAllPathsAsObjects, dataStoreProperties.treatBooleansAsNumeric());
  }
}
