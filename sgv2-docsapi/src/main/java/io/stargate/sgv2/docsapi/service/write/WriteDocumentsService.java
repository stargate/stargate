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
package io.stargate.sgv2.docsapi.service.write;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.api.v2.model.dto.MultiDocsResponse;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.JsonDocumentShredder;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.ReadBridgeService;
import io.stargate.sgv2.docsapi.service.schema.JsonSchemaManager;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class WriteDocumentsService {
  private static final Logger logger = LoggerFactory.getLogger(WriteDocumentsService.class);

  @Inject JsonSchemaManager jsonSchemaManager;

  @Inject JsonDocumentShredder documentShredder;

  @Inject WriteBridgeService writeBridgeService;

  @Inject ReadBridgeService readBridgeService;

  @Inject DocumentConfig configuration;

  /**
   * Writes a document in the given namespace and collection using the randomly generated ID.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param document Document represented as JSON node
   * @param ttl the time-to-live for the document (seconds)
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> writeDocument(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      JsonNode document,
      Integer ttl,
      ExecutionContext context) {
    // generate the document id
    final String documentId = UUID.randomUUID().toString();
    return jsonSchemaManager
        .validateJsonDocument(table, document, false)
        .onItem()
        .transformToUni(
            __ -> {
              List<JsonShreddedRow> rows =
                  documentShredder.shred(document, Collections.emptyList());
              return writeBridgeService
                  .writeDocument(namespace, collection, documentId, rows, ttl, context)
                  .map(
                      result ->
                          new DocumentResponseWrapper<>(
                              documentId, null, null, context.toProfile()));
            });
  }

  /**
   * Writes many documents in the given namespace and collection. If #idPath is not provided, IDs
   * for each document will be randomly generated.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param root Documents represented as JSON array
   * @param idPath Optional path to the id of the document in each doc.
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<MultiDocsResponse> writeDocuments(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      JsonNode root,
      String idPath,
      Integer ttl,
      ExecutionContext context) {
    if (!root.isArray()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_WRITE_BATCH_NOT_ARRAY);
    }
    boolean useUpdate = null != idPath;
    final Optional<JsonPointer> idPointer = DocsApiUtils.pathToJsonPointer(idPath);

    if (idPointer.isPresent()) {
      // Before doing async flow, we have to scan the JSON one time to error out if there are any
      // duplicate ID's
      Set<String> existingIds = new HashSet<>();
      for (JsonNode node : root) {
        String documentId = documentIdResolver().apply(idPointer, node);
        if (existingIds.contains(documentId)) {
          String msg =
              String.format(
                  "Found duplicate ID %s in more than one document when doing batched document write.",
                  documentId);
          throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_WRITE_BATCH_DUPLICATE_ID, msg);
        } else {
          existingIds.add(documentId);
        }
      }
    }

    return Multi.createFrom()
        .iterable(root)
        .onItem()
        .transformToUniAndMerge(
            json -> {
              return jsonSchemaManager
                  .validateJsonDocument(table, json, false)
                  .onItem()
                  .transformToUni(
                      __ -> {
                        String documentId = documentIdResolver().apply(idPointer, json);

                        List<JsonShreddedRow> rows =
                            documentShredder.shred(json, Collections.emptyList());
                        if (useUpdate) {
                          return writeBridgeService
                              .updateDocument(namespace, collection, documentId, rows, ttl, context)
                              .onItemOrFailure()
                              .transform(
                                  (resultSet, failure) -> {
                                    if (failure == null) {
                                      return documentId;
                                    } else {
                                      logger.error(
                                          "Write failed for one of the documents included in the batch document write.",
                                          failure);
                                      return null;
                                    }
                                  });
                        } else {
                          return writeBridgeService
                              .writeDocument(namespace, collection, documentId, rows, ttl, context)
                              .onItemOrFailure()
                              .transform(
                                  (resultSet, failure) -> {
                                    if (failure == null) {
                                      return documentId;
                                    } else {
                                      logger.error(
                                          "Write failed for one of the documents included in the batch document write.",
                                          failure);
                                      return null;
                                    }
                                  });
                        }
                      });
            })
        .collect()
        .asList()
        .onItem()
        .transform(
            ids ->
                new MultiDocsResponse(
                    ids.stream().filter(Objects::nonNull).collect(Collectors.toList()),
                    context.toProfile()));
  }

  /**
   * Updates a document with given ID in the given namespace and collection. Any previously existing
   * document with the same ID will be overwritten.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param payload Document represented as JSON node
   * @param ttl the time-to-live of the document (seconds)
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> updateDocument(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      JsonNode payload,
      Integer ttl,
      ExecutionContext context) {
    return updateDocumentInternal(
        table, namespace, collection, documentId, Collections.emptyList(), payload, ttl, context);
  }

  /**
   * Updates a sub-document with given ID in the given namespace and collection. Any previously
   * existing sub-document with the same ID at the given path will be overwritten.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param payload Document represented as JSON node
   * @param ttlAuto Whether to automatically determine TTL from the surrounding document
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> updateSubDocument(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      JsonNode payload,
      boolean ttlAuto,
      ExecutionContext context) {
    Uni<Integer> ttlValue = Uni.createFrom().item(0);
    if (ttlAuto) {
      ttlValue = determineTtl(namespace, collection, documentId, context);
    }
    return ttlValue
        .onItem()
        .transformToUni(
            ttl ->
                updateDocumentInternal(
                    table, namespace, collection, documentId, subPath, payload, ttl, context));
  }

  /**
   * Updates a document with given ID in the given namespace and collection at the specified
   * sub-path. Any previously existing sub-document at the given path will be overwritten.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param subPath Sub-path of the document to update. If empty will update the whole doc.
   * @param document Document represented as JSON node
   * @param ttl the time-to-live of the document (seconds)
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  private Uni<DocumentResponseWrapper<Void>> updateDocumentInternal(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      JsonNode document,
      Integer ttl,
      ExecutionContext context) {
    final List<String> subPathProcessed = processSubDocumentPath(subPath);
    return jsonSchemaManager
        .validateJsonDocument(table, document, !subPathProcessed.isEmpty())
        .onItem()
        .transformToUni(
            __ -> {
              // shred rows
              List<JsonShreddedRow> rows = documentShredder.shred(document, subPathProcessed);

              // call update document
              return writeBridgeService
                  .updateDocument(
                      namespace, collection, documentId, subPathProcessed, rows, ttl, context)
                  .map(
                      result ->
                          new DocumentResponseWrapper<>(
                              documentId, null, null, context.toProfile()));
            });
  }

  /**
   * Patches a document with given ID in the given namespace and collection. Any previously existing
   * patched keys at the given path will be overwritten, as well as any existing array.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to patch
   * @param payload Document represented as JSON node
   * @param ttlAuto Whether to automatically determine TTL from the surrounding document
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> patchDocument(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      JsonNode payload,
      boolean ttlAuto,
      ExecutionContext context) {
    return patchSubDocument(
        table,
        namespace,
        collection,
        documentId,
        Collections.emptyList(),
        payload,
        ttlAuto,
        context);
  }

  /**
   * Patches a document with given ID in the given namespace and collection at the specified
   * sub-path. Any previously existing patched keys at the given path will be overwritten, as well
   * as any existing array.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to patch
   * @param subPath Sub-path of the document to patch. If empty will patch the whole doc.
   * @param payload Document represented as JSON node
   * @param ttlAuto Whether to automatically determine TTL from the surrounding document
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> patchSubDocument(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      JsonNode payload,
      boolean ttlAuto,
      ExecutionContext context) {
    Uni<Integer> ttlValue = Uni.createFrom().item(0);
    if (ttlAuto) {
      ttlValue = determineTtl(namespace, collection, documentId, context);
    }
    return ttlValue
        .onItem()
        .transformToUni(
            ttl ->
                patchDocumentInternal(
                    table, namespace, collection, documentId, subPath, payload, ttl, context));
  }

  /**
   * Patches a document with given ID in the given namespace and collection at the specified
   * sub-path. Any previously existing patched keys at the given path will be overwritten, as well
   * as any existing array.
   *
   * @param table a CqlTable to be used for schema/validity checks
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to patch
   * @param subPath Sub-path of the document to patch. If empty will patch the whole doc.
   * @param root Document represented as JSON node
   * @param ttl the time-to-live of the document (in seconds), or 'auto'
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  private Uni<DocumentResponseWrapper<Void>> patchDocumentInternal(
      Uni<Schema.CqlTable> table,
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      JsonNode root,
      Integer ttl,
      ExecutionContext context) {
    // pre-process to support array elements
    final List<String> subPathProcessed = processSubDocumentPath(subPath);
    // explicitly forbid arrays and empty objects
    if (root.isArray()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
    }
    if (root.isObject() && root.isEmpty()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PATCH_EMPTY_NOT_ACCEPTED);
    }

    return jsonSchemaManager
        .validateJsonDocument(table, root, !subPathProcessed.isEmpty())
        .onItem()
        .transformToUni(
            __ -> {
              // shred rows
              List<JsonShreddedRow> rows = documentShredder.shred(root, subPathProcessed);

              // call patch document
              return writeBridgeService
                  .patchDocument(
                      namespace, collection, documentId, subPathProcessed, rows, ttl, context)
                  .map(
                      result ->
                          new DocumentResponseWrapper<>(
                              documentId, null, null, context.toProfile()));
            });
  }

  /**
   * Deletes a document with given ID in the given namespace and collection.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to delete
   * @param context Execution content
   * @return Flag representing if the operation was success.
   */
  public Uni<Boolean> deleteDocument(
      String namespace, String collection, String documentId, ExecutionContext context) {
    return writeBridgeService
        .deleteDocument(namespace, collection, documentId, Collections.emptyList(), context)
        .onItem()
        .transform(__ -> true);
  }

  /**
   * Deletes a document with given ID in the given namespace and collection at the specified
   * sub-path. Any previously existing sub-document at the given path will be removed.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to delete
   * @param subPath Sub-path of the document to delete. If empty will delete the whole doc.
   * @param context Execution content
   * @return Flag representing if the operation was success.
   */
  public Uni<Boolean> deleteDocument(
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      ExecutionContext context) {
    // pre-process to support array elements
    List<String> subPathProcessed = processSubDocumentPath(subPath);
    return writeBridgeService
        .deleteDocument(namespace, collection, documentId, subPathProcessed, context)
        .onItem()
        .transform(__ -> true);
  }

  // we need to transform the stuff to support array elements
  private List<String> processSubDocumentPath(List<String> subDocumentPath) {
    return subDocumentPath.stream()
        .map(path -> DocsApiUtils.convertArrayPath(path, configuration.maxArrayLength()))
        .collect(Collectors.toList());
  }

  private Uni<Integer> determineTtl(
      String namespace, String collection, String documentId, ExecutionContext ctx) {
    return readBridgeService
        .getDocumentTtlInfo(namespace, collection, documentId, ctx)
        .onItem()
        .ifNotNull()
        .transform(
            rawDocument -> {
              List<RowWrapper> rows = rawDocument.rows();
              if (rows.isEmpty()) {
                return 0;
              }
              RowWrapper row = rows.get(0);
              String ttlColumn = "ttl(%s)".formatted(configuration.table().leafColumnName());
              if (!row.isNull(ttlColumn)) {
                Long value = row.getLong(ttlColumn);
                return value.intValue();
              }
              return 0;
            })
        .onItem()
        .ifNull()
        .switchTo(() -> Uni.createFrom().item(0));
  }

  // function that resolves a document id, based on the JsonPointer
  // returns random ID if pointer is not provided
  private BiFunction<Optional<JsonPointer>, JsonNode, String> documentIdResolver() {
    return (idPointer, jsonNode) ->
        idPointer
            .map(
                p -> {
                  JsonNode node = jsonNode.at(p);
                  if (!node.isTextual()) {
                    String nodeDes = node.isMissingNode() ? "missing node" : node.toString();
                    String format =
                        String.format(
                            "JSON document %s requires a String value at the path %s in order to resolve document ID, found %s. Batch write failed.",
                            jsonNode, p, nodeDes);
                    throw new ErrorCodeRuntimeException(
                        ErrorCode.DOCS_API_WRITE_BATCH_INVALID_ID_PATH, format);
                  }
                  return node.textValue();
                })
            .orElseGet(() -> UUID.randomUUID().toString());
  }
}
