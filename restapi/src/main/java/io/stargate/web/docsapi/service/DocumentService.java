package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.google.common.base.Splitter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.exception.RuntimeExceptionPassHandlingStrategy;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.docsapi.service.filter.FilterOp;
import io.stargate.web.docsapi.service.filter.ListFilterCondition;
import io.stargate.web.docsapi.service.filter.SingleFilterCondition;
import io.stargate.web.docsapi.service.json.DeadLeafCollectorImpl;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import io.stargate.web.resources.Db;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.PathSegment;
import org.apache.commons.lang3.StringUtils;
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
  private static final Pattern PERIOD_PATTERN = Pattern.compile("\\.");
  private static final Splitter FORM_SPLITTER = Splitter.on('&');
  private static final Splitter PAIR_SPLITTER = Splitter.on('=');
  private static final Splitter PATH_SPLITTER = Splitter.on('/');
  private TimeSource timeSource;
  private DocsApiConfiguration docsApiConfiguration;
  private JsonConverter jsonConverterService;
  private ObjectMapper mapper;
  private DocsSchemaChecker schemaChecker;
  private JsonSchemaHandler jsonSchemaHandler;

  @Inject
  public DocumentService(
      TimeSource timeSource,
      ObjectMapper mapper,
      JsonConverter jsonConverterService,
      DocsApiConfiguration docsApiConfiguration,
      DocsSchemaChecker schemaChecker,
      JsonSchemaHandler jsonSchemaHandler) {
    this.timeSource = timeSource;
    this.mapper = mapper;
    this.jsonConverterService = jsonConverterService;
    this.docsApiConfiguration = docsApiConfiguration;
    this.schemaChecker = schemaChecker;
    this.jsonSchemaHandler = jsonSchemaHandler;
  }

  /*
   * Converts a JSON path string (e.g. "$.a.b.c[0]") into a JSON path string
   * that only uses square brackets to denote pathing (e.g. "$['a']['b']['c'][0]".
   * This is to allow escaping of certain characters, such as space, $, and @.
   */
  private String convertToBracketedPath(String path) {
    String[] parts = PERIOD_PATTERN.split(path);
    StringBuilder newPath = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      if (part.startsWith("$") && i == 0) {
        newPath.append(part);
      } else {
        int indexOfBrace = part.indexOf('[');
        if (indexOfBrace < 0) {
          newPath.append("['").append(part).append("']");
        } else {
          String keyPart = part.substring(0, indexOfBrace);
          String arrayPart = part.substring(indexOfBrace);
          newPath.append("['").append(keyPart).append("']").append(arrayPart);
        }
      }
    }
    return newPath.toString();
  }

  private String leftPadTo6(String value) {
    return StringUtils.leftPad(value, 6, '0');
  }

  private String convertArrayPath(String path) {
    if (path.startsWith("[") && path.endsWith("]")) {
      String innerPath = path.substring(1, path.length() - 1);
      int idx = Integer.parseInt(innerPath);
      if (idx > docsApiConfiguration.getMaxArrayLength() - 1) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
      }
      return "[" + leftPadTo6(innerPath) + "]";
    }
    return path;
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

    surfer
        .configBuilder()
        .bind(
            "$..*",
            (v, parsingContext) -> {
              String fieldName = parsingContext.getCurrentFieldName();
              if (fieldName != null && DocumentDB.containsIllegalChars(fieldName)) {
                String msg =
                    String.format(
                        "The characters %s are not permitted in JSON field names, invalid field %s.",
                        DocumentDB.getForbiddenCharactersMessage(), fieldName);
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME, msg);
              }

              if (v instanceof JsonPrimitive
                  || v instanceof JsonNull
                  || isEmptyObject(v)
                  || isEmptyArray(v)) {
                JsonPath p =
                    JsonPathCompiler.compile(convertToBracketedPath(parsingContext.getJsonPath()));
                int i = path.size();
                Map<String, Object> bindMap = db.newBindMap(path);

                bindMap.put("key", key);

                Iterator<PathOperator> it = p.iterator();
                String leaf = null;
                while (it.hasNext()) {
                  if (i >= docsApiConfiguration.getMaxDepth()) {
                    throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
                  }

                  PathOperator op = it.next();
                  String pv = op.toString();

                  if (pv.equals("$")) continue;

                  // pv always starts with a square brace because of the above conversion
                  String innerPath = pv.substring(1, pv.length() - 1);
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
                    pv = "[" + leftPadTo6(innerPath) + "]";
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
        if (!isArrayElement) {
          // Unlike using JSON, try to allow any input by replacing illegal characters with _.
          // Form shredding is only supposed to be used for benchmarking tests.
          fieldName = DocumentDB.replaceIllegalChars(fieldName);
        }
        if (isArrayElement) {
          if (i == 0 && patching) {
            throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
          }

          String innerPath = fieldName.substring(1, fieldName.length() - 1);
          // Unlike using JSON, try to allow any input by replacing illegal characters with _.
          // Form shredding is only supposed to be used for benchmarking tests.
          innerPath = DocumentDB.replaceIllegalChars(innerPath);

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
          fieldName = "[" + leftPadTo6(innerPath) + "]";
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
      throws UnauthorizedException, JsonProcessingException, ProcessingException {
    DocumentDB db = dbFactory.getDocDataStoreForToken(authToken, headers);
    JsonSurfer surfer = JsonSurferGson.INSTANCE;

    boolean created = db.maybeCreateTable(keyspace, collection);
    // After creating the table, it can take up to 2 seconds for permissions cache to be updated,
    // but we can force the permissions refetch by logging in again.
    if (created) {
      db = dbFactory.getDocDataStoreForToken(authToken, headers);
      db.maybeCreateTableIndexes(keyspace, collection);
    }

    JsonNode schema = jsonSchemaHandler.getCachedJsonSchema(db, keyspace, collection);
    if (schema != null && path.isEmpty()) {
      jsonSchemaHandler.validate(schema, payload);
    } else if (schema != null) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_INVALID_PARTIAL_UPDATE);
    }

    schemaChecker.checkValidity(keyspace, collection, db);

    // Left-pad the path segments that represent arrays
    List<String> convertedPath = new ArrayList<>(path.size());
    for (PathSegment pathSegment : path) {
      String pathStr = pathSegment.getPath();
      convertedPath.add(convertArrayPath(pathStr));
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

  public JsonNode getJsonAtPath(
      DocumentDB db,
      String keyspace,
      String collection,
      String id,
      List<PathSegment> path,
      List<String> fields,
      ExecutionContext context)
      throws UnauthorizedException {

    List<BuiltCondition> predicates = new ArrayList<>();
    predicates.add(BuiltCondition.of("key", Predicate.EQ, id));

    StringBuilder pathStr = new StringBuilder();

    for (int i = 0; i < path.size(); i++) {
      String pathSegment = path.get(i).getPath();
      String convertedPath = convertArrayPath(pathSegment);
      predicates.add(BuiltCondition.of("p" + i, Predicate.EQ, convertedPath));

      if (!pathSegment.equals(convertedPath)) {
        pathStr.append("/").append(pathSegment, 1, pathSegment.length() - 1);
      } else {
        pathStr.append("/").append(pathSegment);
      }
    }

    List<RawDocument> docs =
        db.executeSelect(
                keyspace,
                collection,
                predicates,
                // We have to use a non-zero page size here in case the whole doc does not
                // fit into one page because C* will ignore paging state on subsequent requests
                // if the page size is <= 0.
                // TODO: use a separate page size parameter for full doc queries?
                docsApiConfiguration.getSearchPageSize(),
                null,
                nestedPopulate(context))
            .take(1)
            .toList()
            .blockingGet();

    if (docs.isEmpty()) {
      return null;
    }
    RawDocument rawDoc = docs.get(0);
    List<Row> rows = rawDoc.rows();
    boolean filtered = false;
    if (!fields.isEmpty()) {
      List<String> convertedPath = convertPath(path);
      Collection<List<String>> fullFieldPaths = toFullFieldPaths(convertedPath, fields);
      rows = filterRows(rows, fullFieldPaths);
      filtered = true;
    }

    DeadLeafCollectorImpl collector = new DeadLeafCollectorImpl();
    JsonNode result =
        jsonConverterService.convertToJsonDoc(rows, collector, false, db.treatBooleansAsNumeric());
    // ignore dead leaves in case the rows were pre-filtered
    if (!collector.isEmpty() && !filtered) {
      logger.info(String.format("Deleting %d dead leaves", collector.getLeaves().size()));
      long now = timeSource.currentTimeMicros();
      db.deleteDeadLeaves(keyspace, collection, id, collector.getLeaves(), context, now);
    }
    JsonNode node = result.at(pathStr.toString());
    if (node.isMissingNode()) {
      return null;
    }

    return node;
  }

  private void validateOpAndValue(String op, JsonNode value, String fieldName) {
    Optional<FilterOp> filterOpt = FilterOp.getByRawValue(op);
    // Further down the line, a nicer error message will be made if the filterOp is invalid
    if (!filterOpt.isPresent()) return;
    FilterOp filterOp = filterOpt.get();

    if (filterOp == FilterOp.NE) {
      if (value.isArray() || value.isObject()) {
        String msg =
            String.format(
                "Value entry for field %s, operation %s was expecting a value or `null`",
                fieldName, op);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
      }
    } else if (filterOp == FilterOp.EXISTS) {
      if (!value.isBoolean() || !value.asBoolean()) {
        String msg = String.format("The operation %s only supports the value `true`", op);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
      }
    } else if (filterOp == FilterOp.GT
        || filterOp == FilterOp.GTE
        || filterOp == FilterOp.LT
        || filterOp == FilterOp.LTE
        || filterOp == FilterOp.EQ) {
      if (value.isArray() || value.isObject() || value.isNull()) {
        String msg =
            String.format(
                "Value entry for field %s, operation %s was expecting a non-null value",
                fieldName, op);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
      }
    } else if (filterOp == FilterOp.IN || filterOp == FilterOp.NIN) {
      if (!value.isArray()) {
        String msg =
            String.format(
                "Value entry for field %s, operation %s was expecting an array", fieldName, op);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FILTER_INVALID, msg);
      }
    } else {
      throw new IllegalStateException(String.format("Unknown FilterOp value %s", filterOp));
    }
  }

  public List<String> convertToSelectionList(JsonNode fieldsJson) {
    if (!fieldsJson.isArray()) {
      String msg = String.format("`fields` must be a JSON array, found %s", fieldsJson);
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, msg);
    }

    List<String> res = new ArrayList<>();
    for (int i = 0; i < fieldsJson.size(); i++) {
      JsonNode value = fieldsJson.get(i);
      if (!value.isTextual()) {
        String msg = String.format("Each field must be a string, found %s", value);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, msg);
      }
      res.add(value.asText());
    }

    return res;
  }

  private List<String> convertPath(List<PathSegment> path) {
    return path.stream()
        .map(pathSeg -> DocsApiUtils.convertArrayPath(pathSeg.getPath()))
        .collect(Collectors.toList());
  }

  private Collection<List<String>> toFullFieldPaths(
      List<String> pathPrefix, List<String> filedNames) {
    return filedNames.stream()
        .map(
            name -> {
              List<String> fullFieldPath = new ArrayList<>(pathPrefix.size() + 1);
              fullFieldPath.addAll(pathPrefix);
              fullFieldPath.add(DocsApiUtils.convertArrayPath(name));
              return fullFieldPath;
            })
        .collect(Collectors.toList());
  }

  public List<FilterCondition> convertToFilterOps(
      List<PathSegment> prependedPath, JsonNode filterJson) {
    List<FilterCondition> conditions = new ArrayList<>();

    if (!filterJson.isObject()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_OBJECT_REQUIRED);
    }
    ObjectNode input = (ObjectNode) filterJson;
    Iterator<String> fields = input.fieldNames();
    while (fields.hasNext()) {
      String fieldName = fields.next();
      if (fieldName.isEmpty()) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID);
      }
      String[] fieldNamePath = PERIOD_PATTERN.split(fieldName);
      List<String> convertedFieldNamePath =
          Arrays.asList(fieldNamePath).stream()
              .map(this::convertArrayPath)
              .collect(Collectors.toList());
      if (!prependedPath.isEmpty()) {
        List<String> prependedConverted =
            prependedPath.stream()
                .map(
                    pathSeg -> {
                      String path = pathSeg.getPath();
                      return convertArrayPath(path);
                    })
                .collect(Collectors.toList());
        prependedConverted.addAll(convertedFieldNamePath);
        convertedFieldNamePath = prependedConverted;
      }
      JsonNode fieldConditions = input.get(fieldName);
      if (!fieldConditions.isObject()) {
        String msg =
            String.format(
                "Search entry for field %s was expecting a JSON object as input.", fieldName);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_OBJECT_REQUIRED, msg);
      }

      Iterator<String> ops = fieldConditions.fieldNames();
      while (ops.hasNext()) {
        String op = ops.next();
        JsonNode value = fieldConditions.get(op);
        validateOpAndValue(op, value, fieldName);
        if (value.isNumber()) {
          conditions.add(new SingleFilterCondition(convertedFieldNamePath, op, value.asDouble()));
        } else if (value.isBoolean()) {
          conditions.add(new SingleFilterCondition(convertedFieldNamePath, op, value.asBoolean()));
        } else if (value.isTextual()) {
          conditions.add(new SingleFilterCondition(convertedFieldNamePath, op, value.asText()));
        } else if (value.isArray()) {
          List<Object> valueAsList = new ArrayList<>();
          ArrayNode array = (ArrayNode) value;
          for (int i = 0; i < array.size(); i++) {
            JsonNode n = array.get(i);
            if (n.isInt()) valueAsList.add(n.asInt());
            else if (n.isDouble()) valueAsList.add(n.asDouble());
            else if (n.isBoolean()) valueAsList.add(n.asBoolean());
            else if (n.isTextual()) valueAsList.add(n.asText());
            else if (n.isNull()) valueAsList.add(null);
          }
          conditions.add(new ListFilterCondition(convertedFieldNamePath, op, valueAsList));
        } else {
          conditions.add(new SingleFilterCondition(convertedFieldNamePath, op, (String) null));
        }
      }
    }

    return conditions;
  }

  public void deleteAtPath(
      DocumentDB db, String keyspace, String collection, String id, List<PathSegment> path)
      throws UnauthorizedException {
    List<String> convertedPath = new ArrayList<>(path.size());
    for (PathSegment pathSegment : path) {
      String pathStr = pathSegment.getPath();
      convertedPath.add(convertArrayPath(pathStr));
    }

    long now = timeSource.currentTimeMicros();

    db.delete(keyspace, collection, id, convertedPath, now);
  }

  private FlowableTransformer<RawDocument, RawDocument> filterInMemory(
      DocumentDB db, List<FilterCondition> filters, ExecutionContext context) {
    if (filters.isEmpty()) {
      return flowable -> flowable;
    }

    // Create nested contexts to report in-memory filtering activity
    filters.forEach(f -> nestedInMemory(context, f));

    Set<String> filterFieldPaths =
        filters.stream().map(FilterCondition::getFullFieldPath).collect(Collectors.toSet());

    return flowable ->
        flowable.filter(
            document ->
                matchesFilters(
                    filters, db.treatBooleansAsNumeric(), filterFieldPaths, document.rows()));
  }

  public JsonNode searchDocumentsV2(
      DocumentDB db,
      String keyspace,
      String collection,
      List<PathSegment> pathPrefix,
      List<FilterCondition> filters,
      List<String> fields,
      String documentId,
      Paginator paginator,
      ExecutionContext context)
      throws UnauthorizedException {
    db.authorizeSelect(keyspace, collection);
    List<String> path;
    if (filters.isEmpty()) {
      path =
          pathPrefix.stream()
              .map(seg -> DocsApiUtils.convertArrayPath(seg.getPath()))
              .collect(Collectors.toList());
    } else {
      path = filters.get(0).getPath();
    }

    List<BuiltCondition> conditions = new ArrayList<>();
    conditions.add(BuiltCondition.of("key", Predicate.EQ, documentId));

    int dbPageSize;
    List<FilterCondition> inMemoryFilters;
    ExecutionContext mainContext;
    if (fields.isEmpty() && !filters.isEmpty()) {
      FilterCondition first = filters.get(0);
      conditions.add(BuiltCondition.of("leaf", Predicate.EQ, first.getField()));

      // Assume all filters apply to the same row - this is expected to be enforced by callers
      dbPageSize = paginator.docPageSize;
      List<FilterCondition> inCassandraFilters =
          filters.stream()
              .filter(f -> !FilterOp.LIMITED_SUPPORT_FILTERS.contains(f.getFilterOp()))
              .collect(Collectors.toList());
      inMemoryFilters =
          filters.stream()
              .filter(f -> FilterOp.LIMITED_SUPPORT_FILTERS.contains(f.getFilterOp()))
              .collect(Collectors.toList());

      collectNestedElementConditions(conditions, first.getPath());

      if (!inCassandraFilters.isEmpty()) {
        collectFieldConditions(conditions, inCassandraFilters.get(0));
        mainContext = nestedProperties(context, inCassandraFilters);
      } else {
        mainContext = nestedPopulate(context);
      }

      for (FilterCondition filter : inCassandraFilters) {
        collectValueConditions(conditions, filter, db.treatBooleansAsNumeric());
      }
    } else {
      dbPageSize = docsApiConfiguration.getSearchPageSize();
      inMemoryFilters = filters;

      collectNestedElementConditions(conditions, path);
      mainContext = nestedPopulate(context);
    }

    AbstractBound<?> mainQuery =
        db.builder()
            .select()
            .column(DocumentDB.allColumns())
            .writeTimeColumn("leaf")
            .from(keyspace, collection)
            .where(conditions)
            .allowFiltering()
            .build()
            .bind();

    // Use `key` plus some of the clustering columns (path elements) for grouping query results
    int keyDepth = path.size() + 1; // +1 for the partition key
    List<RawDocument> docs =
        db.executeSelect(
                keyDepth, mainQuery, dbPageSize, paginator.getCurrentDbPageState(), mainContext)
            .compose(filterInMemory(db, inMemoryFilters, context))
            .take(paginator.docPageSize)
            .toList()
            .blockingGet();

    if (docs.isEmpty()) {
      return null;
    }

    paginator.setDocumentPageState(docs);

    ArrayNode docsResult = mapper.createArrayNode();

    for (RawDocument doc : docs) {
      List<Row> rows = doc.rows();
      if (!fields.isEmpty()) {
        Collection<List<String>> fullFieldPaths = toFullFieldPaths(path, fields);
        rows = filterRows(rows, fullFieldPaths);
      }

      JsonNode jsonDoc =
          jsonConverterService.convertToJsonDoc(rows, true, db.treatBooleansAsNumeric());

      docsResult.add(jsonDoc);
    }

    return docsResult;
  }

  private ExecutionContext nestedInMemory(ExecutionContext context, FilterCondition filter) {
    return context.nested(
        "FILTER IN MEMORY: "
            + filter.getFullFieldPath()
            + " "
            + filter.getFilterOp()
            + " "
            + filter.getValue());
  }

  private ExecutionContext nestedProperties(
      ExecutionContext context, List<FilterCondition> filters) {
    return context.nested(
        "LoadProperties: "
            + filters.stream()
                .map(f -> f.getFullFieldPath() + " " + f.getFilterOp() + " " + f.getValue())
                .collect(Collectors.joining(" AND ")));
  }

  private ExecutionContext nestedPopulate(ExecutionContext context) {
    return context.nested("LoadProperties");
  }

  private void collectNestedElementConditions(List<BuiltCondition> predicates, List<String> path) {
    int i;
    for (i = 0; i < path.size(); i++) {
      String[] pathSegmentSplit = path.get(i).split(",");
      if (pathSegmentSplit.length == 1) {
        String pathSegment = pathSegmentSplit[0];
        if (pathSegment.equals(DocumentDB.GLOB_VALUE)) {
          predicates.add(BuiltCondition.of("p" + i, Predicate.GT, ""));
        } else {
          String convertedPath = convertArrayPath(pathSegment);
          predicates.add(BuiltCondition.of("p" + i, Predicate.EQ, convertedPath));
        }
      } else {
        List<String> segmentsList = Arrays.asList(pathSegmentSplit);
        // left pad any array segments to 6 places
        segmentsList =
            segmentsList.stream().map(this::convertArrayPath).collect(Collectors.toList());

        predicates.add(BuiltCondition.of("p" + i, Predicate.IN, segmentsList));
      }
    }
  }

  private void collectFieldConditions(List<BuiltCondition> predicates, FilterCondition condition) {
    predicates.add(
        BuiltCondition.of("p" + condition.getPath().size(), Predicate.EQ, condition.getField()));
  }

  private void collectValueConditions(
      List<BuiltCondition> predicates, FilterCondition condition, boolean booleanAsNumeric) {
    if (condition.getFilterOp() == FilterOp.EXISTS) {
      // conditions on the clustering key columns are sufficient for `EXISTS` as the storage leyer
      return;
    }

    // Only SingleFilterCondition is currently supported at persistence level
    SingleFilterCondition singleFilter = (SingleFilterCondition) condition;
    FilterOp queryOp = singleFilter.getFilterOp();
    String queryValueField = singleFilter.getValueColumnName();
    Object queryValue = singleFilter.getValue(booleanAsNumeric);
    BuiltCondition filterCondition =
        BuiltCondition.of(queryValueField, queryOp.predicate, queryValue);
    predicates.add(filterCondition);
  }

  private String getParentPathFromRow(Row row) {
    int i = 0;
    StringBuilder s = new StringBuilder();
    boolean end = false;
    s.append(row.getString("key")).append("/");
    while (i < docsApiConfiguration.getMaxDepth() && !end) {
      String pathSegment = row.getString("p" + i);
      String nextPathSegment =
          i + 1 < docsApiConfiguration.getMaxDepth() ? row.getString("p" + (i + 1)) : null;
      end = (nextPathSegment == null || nextPathSegment.equals(""));
      if (!end) {
        s.append(pathSegment).append(".");
        i++;
      }
    }
    return s.toString();
  }

  private boolean pathsMatch(String path1, String path2) {
    String[] parts1 = PERIOD_PATTERN.split(path1);
    String[] parts2 = PERIOD_PATTERN.split(path2);
    if (parts1.length != parts2.length) {
      return false;
    }

    for (int i = 0; i < parts1.length; i++) {
      String part1 = parts1[i];
      String part2 = parts2[i];
      if (!part1.equals("*") && !part2.equals("*") && !part1.equals(part2)) {
        return false;
      }
    }
    return true;
  }

  private List<Row> filterRows(List<Row> rows, Collection<List<String>> fullFieldPaths) {
    return rows.stream()
        .filter(
            r ->
                fullFieldPaths.stream()
                    .anyMatch(fieldPath -> DocsApiUtils.isRowMatchingPath(r, fieldPath)))
        .collect(Collectors.toList());
  }

  private boolean matchesFilters(
      List<FilterCondition> inMemoryFilters,
      boolean numericBooleans,
      Set<String> filterFieldPaths,
      List<Row> docChunk) {
    List<Row> fieldRows =
        docChunk.stream()
            .filter(
                r -> {
                  if (r == null || r.getString("leaf") == null) {
                    return false;
                  }
                  List<String> parentPath = PATH_SPLITTER.splitToList(getParentPathFromRow(r));
                  String rowPath = "";
                  if (parentPath.size() == 2) {
                    rowPath = parentPath.get(1);
                  }
                  String fullRowPath = rowPath + r.getString("leaf");
                  List<FilterCondition> matchingFilters =
                      inMemoryFilters.stream()
                          .filter(f -> pathsMatch(fullRowPath, f.getFullFieldPath()))
                          .collect(Collectors.toList());
                  if (matchingFilters.isEmpty()) {
                    return false;
                  }
                  return allFiltersMatch(r, matchingFilters, numericBooleans);
                })
            .collect(Collectors.toList());
    // This ensures that wildcard paths are properly counted with non-wildcard paths,
    // by making sure that for every filter above, at least one matches a valid row.
    return filterFieldPaths.stream()
        .allMatch(
            fieldPath ->
                fieldRows.stream()
                    .anyMatch(
                        row -> {
                          List<String> segments =
                              PATH_SPLITTER.splitToList(getParentPathFromRow(row));
                          String path = segments.get(segments.size() - 1);
                          return pathsMatch(path + row.getString("leaf"), fieldPath);
                        }));
  }

  private boolean allFiltersMatch(Row row, List<FilterCondition> filters, boolean numericBooleans) {
    String textValue = DocsApiUtils.getStringFromRow(row);
    Boolean boolValue = DocsApiUtils.getBooleanFromRow(row, numericBooleans);
    Double dblValue = DocsApiUtils.getDoubleFromRow(row);
    for (FilterCondition fc : filters) {
      if (fc.getFilterOp() == FilterOp.EXISTS) {
        if (textValue == null && boolValue == null && dblValue == null) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.EQ) {
        Boolean res = checkEqualsOp((SingleFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || !res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.NE) {
        Boolean res = checkEqualsOp((SingleFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.IN) {
        Boolean res = checkInOp((ListFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || !res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.NIN) {
        Boolean res = checkInOp((ListFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.LTE) {
        Boolean res = checkGtOp((SingleFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.GT) {
        Boolean res = checkGtOp((SingleFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || !res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.LT) {
        Boolean res = checkLtOp((SingleFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || !res) {
          return false;
        }
      } else if (fc.getFilterOp() == FilterOp.GTE) {
        Boolean res = checkLtOp((SingleFilterCondition) fc, textValue, boolValue, dblValue);
        if (res == null || res) {
          return false;
        }
      } else {
        throw new IllegalStateException(
            String.format("Invalid Filter Operation: %s", fc.getFilterOp()));
      }
    }
    return true;
  }

  private Boolean checkEqualsOp(
      SingleFilterCondition filterCondition, String textValue, Boolean boolValue, Double dblValue) {
    boolean boolValueEqual = Objects.equals(filterCondition.getBooleanValue(), boolValue);
    boolean dblValueEqual =
        Objects.equals(filterCondition.getDoubleValue(), dblValue)
            || (filterCondition.getDoubleValue() != null
                && dblValue != null
                && Math.abs(filterCondition.getDoubleValue() - dblValue) < .000001);
    boolean textValueEqual = StringUtils.equals(filterCondition.getTextValue(), textValue);
    return boolValueEqual && dblValueEqual && textValueEqual;
  }

  private Boolean checkInOp(
      ListFilterCondition filterCondition, String textValue, Boolean boolValue, Double dblValue) {
    List<Object> value = filterCondition.getValue();
    if (boolValue != null) {
      return value.stream()
          .anyMatch(
              v -> {
                if (!(v instanceof Boolean) || v == null) return false;
                return (boolean) v == boolValue;
              });
    } else if (dblValue != null) {
      return value.stream()
          .anyMatch(
              v -> {
                if (!(v instanceof Double || v instanceof Integer) || v == null) return false;
                Double dbl;
                if (v instanceof Integer) {
                  dbl = Double.valueOf((Integer) v);
                } else {
                  dbl = (Double) v;
                }
                return Math.abs(dbl - dblValue) < .000001;
              });
    } else {
      return value.stream()
          .anyMatch(
              v -> {
                if (v != null && !(v instanceof String)) return false;
                return StringUtils.equals((String) v, textValue);
              });
    }
  }

  private Boolean checkGtOp(
      SingleFilterCondition filterCondition, String textValue, Boolean boolValue, Double dblValue) {
    Object value = filterCondition.getValue();
    if (boolValue != null) {
      // In cassandra, True is greater than False
      if (!(value instanceof Boolean) || value == null) return null;
      return boolValue && !((boolean) value);
    } else if (dblValue != null) {
      if (!(value instanceof Double || value instanceof Integer) || value == null) return null;
      Double dbl;
      if (value instanceof Integer) {
        dbl = Double.valueOf((Integer) value);
      } else {
        dbl = (Double) value;
      }
      return dblValue - dbl > .000001;
    } else {
      if (!(value instanceof String) || value == null || textValue == null) return null;
      return ((String) value).compareTo(textValue) < 0;
    }
  }

  private Boolean checkLtOp(
      SingleFilterCondition filterCondition, String textValue, Boolean boolValue, Double dblValue) {
    Object value = filterCondition.getValue();
    if (boolValue != null) {
      // In cassandra, False is less than True
      if (!(value instanceof Boolean) || value == null) return null;
      return !boolValue && ((boolean) value);
    } else if (dblValue != null) {
      if (!(value instanceof Double || value instanceof Integer) || value == null) return null;
      Double dbl;
      if (value instanceof Integer) {
        dbl = Double.valueOf((Integer) value);
      } else {
        dbl = (Double) value;
      }
      return dbl - dblValue > .000001;
    } else {
      if (!(value instanceof String) || value == null || textValue == null) return null;
      return ((String) value).compareTo(textValue) > 0;
    }
  }
}
