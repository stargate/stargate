package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.google.common.base.Splitter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.stargate.auth.UnauthorizedException;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.exception.RuntimeExceptionPassHandlingStrategy;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import io.stargate.web.resources.Db;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferGson;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.path.JsonPath;
import org.jsfr.json.path.PathOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentService {
  private static final Logger logger = LoggerFactory.getLogger(DocumentService.class);
  private static final Pattern PERIOD_PATTERN = Pattern.compile("(?<!\\\\)\\.");
  private static final Splitter FORM_SPLITTER = Splitter.on('&');
  private static final Splitter PAIR_SPLITTER = Splitter.on('=');

  private final TimeSource timeSource;
  private final DocsApiConfiguration docsApiConfiguration;
  private final ObjectMapper mapper;
  private final DocsSchemaChecker schemaChecker;
  private final JsonSchemaHandler jsonSchemaHandler;

  @Inject
  public DocumentService(
      TimeSource timeSource,
      ObjectMapper mapper,
      DocsApiConfiguration docsApiConfiguration,
      DocsSchemaChecker schemaChecker,
      JsonSchemaHandler jsonSchemaHandler) {
    this.timeSource = timeSource;
    this.mapper = mapper;
    this.docsApiConfiguration = docsApiConfiguration;
    this.schemaChecker = schemaChecker;
    this.jsonSchemaHandler = jsonSchemaHandler;
  }

  private boolean isEmptyObject(Object v) {
    return v instanceof JsonElement
        && ((JsonElement) v).isJsonObject()
        && ((JsonObject) v).size() == 0;
  }

  private boolean isEmptyArray(Object v) {
    return v instanceof JsonElement
        && ((JsonElement) v).isJsonArray()
        && ((JsonArray) v).size() == 0;
  }

  /**
   * Transforms a JSON payload into a set of bind variables to send to Cassandra.
   *
   * @param surfer
   * @param db
   * @param path The path within the document that is being written to. If empty, writes to the root
   *     of the document.
   * @param key The name of the document that will be written
   * @param payload a JSON object, or a URL-encoded form with the relevant data in it
   * @param patching If this payload meant to be part of a PATCH request (this causes a small amount
   *     of extra validation if true)
   * @param isJson if the request had a content type of application/json, else it will be
   *     interpreted as a URL encoded form
   * @return The full bind variable list for the subsequent inserts, and all first-level keys, as an
   *     ImmutablePair.
   */
  private ImmutablePair<List<Object[]>, List<String>> shredPayload(
      JsonSurfer surfer,
      DocumentDB db,
      List<String> path,
      String key,
      String payload,
      boolean patching,
      boolean isJson) {
    String trimmed = payload.trim();
    if (isJson) {
      return shredJson(surfer, db, path, key, trimmed, patching);
    } else {
      return shredForm(db, path, key, trimmed, patching);
    }
  }

  private ImmutablePair<List<Object[]>, List<String>> shredJson(
      JsonSurfer surfer,
      DocumentDB db,
      List<String> path,
      String key,
      String jsonPayload,
      boolean patching) {
    List<Object[]> bindVariableList = new ArrayList<>();
    List<String> firstLevelKeys = new ArrayList<>();
    try {
      surfer
          .configBuilder()
          .bind(
              "$..*",
              (v, parsingContext) -> {
                String fieldName = parsingContext.getCurrentFieldName();
                if (fieldName != null && DocumentDB.containsIllegalSequences(fieldName)) {
                  String msg =
                      String.format(
                          "Array paths contained in square brackets, periods, and single quotes are not allowed in field names, invalid field %s. Hint: Use unicode escape sequences to encode these characters.",
                          fieldName);
                  throw new ErrorCodeRuntimeException(
                      ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME, msg);
                }

                if (v instanceof JsonPrimitive
                    || v instanceof JsonNull
                    || isEmptyObject(v)
                    || isEmptyArray(v)) {
                  JsonPath p =
                      JsonPathCompiler.compile(
                          DocsApiUtils.convertJsonToBracketedPath(parsingContext.getJsonPath()));
                  int i = path.size();
                  Map<String, Object> bindMap = db.newBindMap(path);

                  bindMap.put("key", key);

                  Iterator<PathOperator> it = p.iterator();
                  String leaf = null;
                  while (it.hasNext()) {
                    if (i >= docsApiConfiguration.getMaxDepth()) {
                      throw new ErrorCodeRuntimeException(
                          ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
                    }

                    PathOperator op = it.next();
                    String pv = op.toString();

                    if (pv.equals("$")) continue;

                    // pv always starts with a square brace because of the above conversion
                    String innerPath =
                        DocsApiUtils.convertEscapedCharacters(pv.substring(1, pv.length() - 1));
                    boolean isArrayElement = op.getType() == PathOperator.Type.ARRAY;
                    if (isArrayElement) {
                      if (i == path.size() && patching) {
                        throw new ErrorCodeRuntimeException(
                            ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
                      }

                      int idx = Integer.parseInt(innerPath);
                      if (idx > docsApiConfiguration.getMaxArrayLength() - 1) {
                        throw new ErrorCodeRuntimeException(
                            ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
                      }

                      // left-pad the array element to 6 characters
                      pv = "[" + DocsApiUtils.leftPadTo6(innerPath) + "]";
                    } else if (i == path.size()) {
                      firstLevelKeys.add(innerPath);
                      pv = innerPath;
                    } else {
                      pv = innerPath;
                    }

                    bindMap.put("p" + i++, pv);
                    leaf = pv;
                  }

                  bindMap.put("leaf", leaf);

                  if (v instanceof JsonPrimitive) {
                    JsonPrimitive value = (JsonPrimitive) v;

                    if (value.isNumber()) {
                      bindMap.put("dbl_value", value.getAsDouble());
                      bindMap.put("bool_value", null);
                      bindMap.put("text_value", null);
                    } else if (value.isBoolean()) {
                      bindMap.put("dbl_value", null);
                      bindMap.put(
                          "bool_value",
                          convertToBackendBooleanValue(
                              value.getAsBoolean(), db.treatBooleansAsNumeric()));
                      bindMap.put("text_value", null);
                    } else {
                      bindMap.put("dbl_value", null);
                      bindMap.put("bool_value", null);
                      bindMap.put("text_value", value.getAsString());
                    }
                  } else if (isEmptyObject(v)) {
                    bindMap.put("dbl_value", null);
                    bindMap.put("bool_value", null);
                    bindMap.put("text_value", DocumentDB.EMPTY_OBJECT_MARKER);
                  } else if (isEmptyArray(v)) {
                    bindMap.put("dbl_value", null);
                    bindMap.put("bool_value", null);
                    bindMap.put("text_value", DocumentDB.EMPTY_ARRAY_MARKER);
                  } else {
                    bindMap.put("dbl_value", null);
                    bindMap.put("bool_value", null);
                    bindMap.put("text_value", null);
                  }

                  logger.debug("{}", bindMap.values());
                  bindVariableList.add(bindMap.values().toArray());
                }
              })
          .withErrorStrategy(new RuntimeExceptionPassHandlingStrategy())
          .buildAndSurf(jsonPayload);
      return ImmutablePair.of(bindVariableList, firstLevelKeys);
    } catch (RuntimeException e) {
      if (e instanceof ErrorCodeRuntimeException) {
        throw e;
      }
      logger.error("Error occurred during JSON read", e);
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Malformed JSON object found during read.", e);
    }
  }

  private Object convertToBackendBooleanValue(boolean value, boolean numericBooleans) {
    if (numericBooleans) {
      return value ? 1 : 0;
    }
    return value;
  }

  private ImmutablePair<List<Object[]>, List<String>> shredForm(
      DocumentDB db, List<String> path, String key, String formPayload, boolean patching) {
    List<Object[]> bindVariableList = new ArrayList<>();
    List<String> firstLevelKeys = new ArrayList<>();
    Iterable<String> pairs = FORM_SPLITTER.split(formPayload);
    for (String pair : pairs) {
      List<String> data = PAIR_SPLITTER.splitToList(pair);
      String fullyQualifiedField;
      String value;
      if (data.size() == 2) {
        fullyQualifiedField = data.get(0);
        value = data.get(1);
      } else if (data.size() == 1) {
        fullyQualifiedField = "data";
        value = data.get(0);
      } else {
        continue;
      }
      String[] fieldNames = PERIOD_PATTERN.split(fullyQualifiedField);

      if (path.size() + fieldNames.length > docsApiConfiguration.getMaxDepth()) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
      }

      Map<String, Object> bindMap = db.newBindMap(path);
      bindMap.put("key", key);

      String leaf = null;
      for (int i = 0; i < fieldNames.length; i++) {
        String fieldName = fieldNames[i];
        boolean isArrayElement = fieldName.startsWith("[") && fieldName.endsWith("]");
        if (isArrayElement) {
          if (i == 0 && patching) {
            throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
          }

          String innerPath = fieldName.substring(1, fieldName.length() - 1);

          int idx = 0;
          try {
            idx = Integer.parseInt(innerPath);
          } catch (NumberFormatException e) {
            // do nothing
          }
          if (idx > docsApiConfiguration.getMaxArrayLength() - 1) {
            throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
          }

          // left-pad the array element to 6 characters
          fieldName = "[" + DocsApiUtils.leftPadTo6(innerPath) + "]";
        } else if (i == 0) {
          firstLevelKeys.add(fieldName);
        }

        bindMap.put("p" + (i + path.size()), fieldName);
        leaf = fieldName;
      }
      bindMap.put("leaf", leaf);

      if (value.equals("null")) {
        bindMap.put("dbl_value", null);
        bindMap.put("bool_value", null);
        bindMap.put("text_value", null);
      } else if (value.equals("true") || value.equals("false")) {
        bindMap.put("dbl_value", null);
        bindMap.put(
            "bool_value",
            convertToBackendBooleanValue(Boolean.parseBoolean(value), db.treatBooleansAsNumeric()));
        bindMap.put("text_value", null);
      } else {
        boolean isNumber;
        Double doubleValue = null;
        try {
          doubleValue = Double.parseDouble(value);
          isNumber = true;
        } catch (NumberFormatException e) {
          isNumber = false;
        }
        if (isNumber) {
          bindMap.put("dbl_value", doubleValue);
          bindMap.put("bool_value", null);
          bindMap.put("text_value", null);
        } else {
          bindMap.put("dbl_value", null);
          bindMap.put("bool_value", null);
          bindMap.put("text_value", value);
        }
      }
      logger.debug("{}", bindMap.values());
      bindVariableList.add(bindMap.values().toArray());
    }
    return ImmutablePair.of(bindVariableList, firstLevelKeys);
  }

  private Optional<String> convertToJsonPtr(Optional<String> path) {
    return path.map(
        p -> "/" + p.replaceAll(PERIOD_PATTERN.pattern(), "/").replaceAll("\\[(\\d+)\\]", "$1"));
  }

  private DocumentDB maybeCreateTableAndIndexes(
      Db dbFactory,
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
      db = dbFactory.getDocDataStoreForToken(authToken, headers);
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
      Db dbFactory,
      ExecutionContext context,
      Map<String, String> headers)
      throws IOException, UnauthorizedException {

    DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, headers);
    JsonSurfer surfer = JsonSurferGson.INSTANCE;

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
                        shredJson(
                                surfer,
                                finalDb,
                                Collections.emptyList(),
                                data.getKey(),
                                data.getValue(),
                                false)
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
      Db dbFactory,
      boolean isJson,
      Map<String, String> headers,
      ExecutionContext context)
      throws UnauthorizedException, ProcessingException {
    DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, headers);
    JsonSurfer surfer = JsonSurferGson.INSTANCE;

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
      convertedPath.add(DocsApiUtils.convertArrayPath(pathStr));
    }

    ImmutablePair<List<Object[]>, List<String>> shreddingResults =
        shredPayload(surfer, db, convertedPath, id, payload, patching, isJson);

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

  public void deleteAtPath(
      DocumentDB db, String keyspace, String collection, String id, List<PathSegment> path)
      throws UnauthorizedException {
    List<String> convertedPath = new ArrayList<>(path.size());
    for (PathSegment pathSegment : path) {
      String pathStr = pathSegment.getPath();
      convertedPath.add(DocsApiUtils.convertArrayPath(pathStr));
    }

    long now = timeSource.currentTimeMicros();

    db.delete(keyspace, collection, id, convertedPath, now);
  }
}
