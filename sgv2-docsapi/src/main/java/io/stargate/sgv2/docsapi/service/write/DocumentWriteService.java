package io.stargate.sgv2.docsapi.service.write;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.stargate.sgv2.docsapi.service.schema.JsonSchemaManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class DocumentWriteService {
  @Inject ObjectMapper objectMapper;

  @Inject JsonSchemaManager jsonSchemaManager;

  @Inject TableManager tableManager;

  @Inject JsonDocumentShredder documentShredder;

  @Inject BridgeWriteService bridgeWriteService;

  @Inject DocumentConfig configuration;

  /**
   * Writes a document in the given namespace and collection using the randomly generated ID.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param payload Document represented as JSON string
   * @param ttl the time-to-live for the document (seconds)
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> writeDocument(
      String namespace, String collection, String payload, Integer ttl, ExecutionContext context) {
    // generate the document id
    final String documentId = UUID.randomUUID().toString();
    final JsonNode document = readPayload(payload);
    return jsonSchemaManager
        .validateJsonDocument(
            tableManager.getValidCollectionTable(namespace, collection), document, false)
        .map(
            valid -> {
              if (!valid) {
                throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
              }
              return true;
            })
        .flatMap(
            __ -> {
              List<JsonShreddedRow> rows =
                  documentShredder.shred(document, Collections.emptyList());
              return bridgeWriteService.writeDocument(
                  namespace, collection, documentId, rows, ttl, context);
            })
        .map(any -> new DocumentResponseWrapper<>(documentId, null, null, context.toProfile()));
  }

  /**
   * Writes many documents in the given namespace and collection. If #idPath is not provided, IDs
   * for each document will be randomly generated.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param payload Documents represented as JSON array
   * @param idPath Optional path to the id of the document in each doc.
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<MultiDocsResponse> writeDocuments(
      String namespace,
      String collection,
      String payload,
      String idPath,
      Integer ttl,
      ExecutionContext context) {
    final JsonNode root = readPayload(payload);
    if (!root.isArray()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_WRITE_BATCH_NOT_ARRAY);
    }
    boolean useUpdate = null != idPath;

    Uni<Schema.CqlTable> table = tableManager.getValidCollectionTable(namespace, collection);
    // keep order with LinkedHashMap
    LinkedHashMap<String, List<JsonShreddedRow>> documentRowsMap = new LinkedHashMap<>();
    Optional<JsonPointer> idPointer = DocsApiUtils.pathToJsonPointer(idPath);

    for (JsonNode documentNode : root) {
      // validate that the document fits the schema
      jsonSchemaManager
          .validateJsonDocument(table, documentNode, false)
          .map(
              valid -> {
                if (!valid) {
                  throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
                }
                return true;
              })
          .invoke(
              __ -> {
                // get document id
                String documentId = documentIdResolver().apply(idPointer, documentNode);
                // shred rows
                List<JsonShreddedRow> rows =
                    documentShredder.shred(documentNode, Collections.emptyList());

                // add to map and make sure we did not have already the same ID
                if (documentRowsMap.put(documentId, rows) != null) {
                  String msg =
                      String.format(
                          "Found duplicate ID %s in more than one document when doing batched document write.",
                          documentId);
                  throw new ErrorCodeRuntimeException(
                      ErrorCode.DOCS_API_WRITE_BATCH_DUPLICATE_ID, msg);
                }
              });
    }

    // TODO how to do this fully non-blocking?
    List<String> documentIds =
        documentRowsMap.entrySet().stream()
            .map(
                entry -> {
                  String documentId = entry.getKey();
                  List<JsonShreddedRow> documentRows = entry.getValue();

                  // use write when possible to avoid the extra delete query
                  if (useUpdate) {
                    return bridgeWriteService
                        .updateDocument(
                            namespace,
                            collection,
                            documentId,
                            Collections.emptyList(),
                            documentRows,
                            ttl,
                            context)
                        .onItem()
                        .transform(resultSet -> documentId);
                  } else {
                    return bridgeWriteService
                        .writeDocument(
                            namespace, collection, documentId, documentRows, ttl, context)
                        .onItem()
                        .transform(resultSet -> documentId);
                  }
                })
            .map(uni -> uni.await().indefinitely())
            .collect(Collectors.toList());

    return Uni.createFrom().item(new MultiDocsResponse(documentIds, context.toProfile()));
  }

  /**
   * Updates a document with given ID in the given namespace and collection. Any previously existing
   * document with the same ID will be overwritten.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param payload Document represented as JSON string
   * @param ttl the time-to-live of the document (seconds)
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  public Uni<DocumentResponseWrapper<Void>> updateDocument(
      String namespace,
      String collection,
      String documentId,
      String payload,
      Integer ttl,
      ExecutionContext context) {
    return updateDocumentInternal(
        namespace, collection, documentId, Collections.emptyList(), payload, ttl, context);
  }

  /**
   * Updates a document with given ID in the given namespace and collection at the specified
   * sub-path. Any previously existing sub-document at the given path will be overwritten.
   *
   * @param namespace Namespace
   * @param collection Collection name
   * @param documentId The ID of the document to update
   * @param subPath Sub-path of the document to update. If empty will update the whole doc.
   * @param payload Document represented as JSON string
   * @param ttl the time-to-live of the document (seconds)
   * @param context Execution content
   * @return Document response wrapper containing the generated ID.
   */
  private Uni<DocumentResponseWrapper<Void>> updateDocumentInternal(
      String namespace,
      String collection,
      String documentId,
      List<String> subPath,
      String payload,
      Integer ttl,
      ExecutionContext context) {
    final JsonNode document = readPayload(payload);
    final List<String> subPathProcessed = processSubDocumentPath(subPath);
    return jsonSchemaManager
        .validateJsonDocument(
            tableManager.getValidCollectionTable(namespace, collection),
            document,
            !subPathProcessed.isEmpty())
        .onItem()
        .transform(
            valid -> {
              if (!valid) {
                throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_INVALID_JSON_VALUE);
              }
              return true;
            })
        .onItem()
        .transform(
            __ -> {
              // shred rows
              List<JsonShreddedRow> rows = documentShredder.shred(document, subPathProcessed);

              // call write document
              return bridgeWriteService.updateDocument(
                  namespace, collection, documentId, subPathProcessed, rows, ttl, context);
            })
        .map(any -> new DocumentResponseWrapper<>(documentId, null, null, context.toProfile()));
  }

  // we need to transform the stuff to support array elements
  private List<String> processSubDocumentPath(List<String> subDocumentPath) {
    return subDocumentPath.stream()
        .map(path -> DocsApiUtils.convertArrayPath(path, configuration.maxArrayLength()))
        .collect(Collectors.toList());
  }

  private JsonNode readPayload(String payload) {
    try {
      return objectMapper.readTree(payload);
    } catch (JsonProcessingException e) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE,
          "Malformed JSON object found during read: " + e,
          e);
    }
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
