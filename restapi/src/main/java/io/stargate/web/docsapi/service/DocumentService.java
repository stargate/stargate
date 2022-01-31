package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import io.stargate.auth.UnauthorizedException;
import io.stargate.core.util.TimeSource;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.DocumentDBFactory;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentService {
  private static final Logger logger = LoggerFactory.getLogger(DocumentService.class);

  private final TimeSource timeSource;
  private final ObjectMapper mapper;
  private final DocsSchemaChecker schemaChecker;
  private final JsonSchemaHandler jsonSchemaHandler;
  private final DocsShredder docsShredder;
  private final DocsApiConfiguration config;

  @Inject
  public DocumentService(
      TimeSource timeSource,
      ObjectMapper mapper,
      DocsSchemaChecker schemaChecker,
      JsonSchemaHandler jsonSchemaHandler,
      DocsShredder docsShredder,
      DocsApiConfiguration config) {
    this.timeSource = timeSource;
    this.mapper = mapper;
    this.schemaChecker = schemaChecker;
    this.jsonSchemaHandler = jsonSchemaHandler;
    this.docsShredder = docsShredder;
    this.config = config;
  }

  private Optional<String> convertToJsonPtr(Optional<String> path) {
    return path.map(
        p ->
            "/"
                + p.replaceAll(DocsApiUtils.PERIOD_PATTERN.pattern(), "/")
                    .replaceAll("\\[(\\d+)\\]", "$1"));
  }

  private DocumentDB maybeCreateTableAndIndexes(
      DocumentDBFactory dbFactory,
      DocumentDB db,
      String keyspace,
      String collection,
      Map<String, String> headers,
      String authToken)
      throws UnauthorizedException {
    boolean created = db.maybeCreateTable(keyspace, collection);
    // After creating the table, it can take up to 2 seconds for permissions cache to be updated,
    // but we can force the permissions refetch by logging in again.
    if (created) {
      db = dbFactory.getDocDBForToken(authToken, headers);
      db.maybeCreateTableIndexes(keyspace, collection);
    }
    return db;
  }

  public List<String> writeManyDocs(
      String authToken,
      String keyspace,
      String collection,
      InputStream payload,
      Optional<String> idPath,
      DocumentDBFactory dbFactory,
      ExecutionContext context,
      Map<String, String> headers)
      throws IOException, UnauthorizedException {

    DocumentDB db = dbFactory.getDocDBForToken(authToken, headers);
    JsonSurfer surfer = JsonSurferJackson.INSTANCE;

    db = maybeCreateTableAndIndexes(dbFactory, db, keyspace, collection, headers, authToken);
    List<String> idsWritten = new ArrayList<>();
    try (JsonParser jsonParser = mapper.getFactory().createParser(payload)) {
      Optional<String> docsPath = convertToJsonPtr(idPath);

      Map<String, String> docs = new LinkedHashMap<>();
      if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
        throw new IllegalArgumentException("Payload must be an array.");
      }

      while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
        JsonNode json;
        try {
          json = mapper.readTree(jsonParser);
        } catch (JsonProcessingException e) {
          throw new ErrorCodeRuntimeException(
              ErrorCode.DOCS_API_INVALID_JSON_VALUE,
              "Malformed JSON encountered during batch write.");
        }
        String docId;
        if (docsPath.isPresent()) {
          if (!json.at(docsPath.get()).isTextual()) {
            throw new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_WRITE_BATCH_INVALID_ID_PATH,
                String.format(
                    "Json Document %s requires a String value at the path %s, found %s."
                        + " Batch write failed.",
                    json, idPath.get(), json.at(docsPath.get()).toString()));
          }
          docId = json.requiredAt(docsPath.get()).asText();
        } else {
          docId = UUID.randomUUID().toString();
        }
        docs.put(docId, json.toString());
      }

      List<Object[]> bindVariableList = new ArrayList<>();
      DocumentDB finalDb = db;
      List<String> ids =
          docs.entrySet().stream()
              .map(
                  data -> {
                    bindVariableList.addAll(
                        docsShredder.shredJson(
                                surfer,
                                Collections.emptyList(),
                                data.getKey(),
                                data.getValue(),
                                false,
                                finalDb.treatBooleansAsNumeric())
                            .left);
                    return data.getKey();
                  })
              .collect(Collectors.toList());

      long now = timeSource.currentTimeMicros();
      try {
        db.deleteManyThenInsertBatch(
            keyspace,
            collection,
            ids,
            bindVariableList,
            Collections.emptyList(),
            now,
            context.nested("ASYNC INSERT"));
      } catch (Exception e) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_WRITE_BATCH_FAILED);
      }

      idsWritten.addAll(ids);
    }
    return idsWritten;
  }

  public void putAtPath(
      String authToken,
      String keyspace,
      String collection,
      String id,
      String payload,
      List<PathSegment> path,
      boolean patching,
      DocumentDBFactory dbFactory,
      boolean isJson,
      Map<String, String> headers,
      ExecutionContext context)
      throws UnauthorizedException, ProcessingException {
    DocumentDB db = dbFactory.getDocDBForToken(authToken, headers);
    JsonSurfer surfer = JsonSurferJackson.INSTANCE;

    db = maybeCreateTableAndIndexes(dbFactory, db, keyspace, collection, headers, authToken);

    JsonNode schema = jsonSchemaHandler.getCachedJsonSchema(db, keyspace, collection);
    if (schema != null && path.isEmpty() && isJson) {
      jsonSchemaHandler.validate(schema, payload);
    } else if (schema != null) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_INVALID_PARTIAL_UPDATE);
    }

    schemaChecker.checkValidity(keyspace, collection, db);

    // Left-pad the path segments that represent arrays
    List<String> convertedPath = new ArrayList<>(path.size());
    for (PathSegment pathSegment : path) {
      String pathStr = pathSegment.getPath();
      convertedPath.add(DocsApiUtils.convertArrayPath(pathStr, config.getMaxArrayLength()));
    }

    ImmutablePair<List<Object[]>, List<String>> shreddingResults =
        docsShredder.shredPayload(
            surfer, convertedPath, id, payload, patching, db.treatBooleansAsNumeric(), isJson);

    List<Object[]> bindVariableList = shreddingResults.left;
    List<String> firstLevelKeys = shreddingResults.right;

    if (bindVariableList.isEmpty() && isJson) {
      String msg =
          "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: "
              + payload
              + ". Hint: update the parent path with a defined object instead.";
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PUT_PAYLOAD_INVALID, msg);
    }

    logger.debug("Bind {}", bindVariableList.size());

    long now = timeSource.currentTimeMicros();
    if (patching) {
      db.deletePatchedPathsThenInsertBatch(
          keyspace,
          collection,
          id,
          bindVariableList,
          convertedPath,
          firstLevelKeys,
          now,
          context.nested("ASYNC PATCH"));
    } else {
      db.deleteThenInsertBatch(
          keyspace,
          collection,
          id,
          bindVariableList,
          convertedPath,
          now,
          context.nested("ASYNC INSERT"));
    }
  }
}
