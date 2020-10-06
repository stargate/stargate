package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.*;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.ImmutableWhereCondition;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.datastore.query.WhereCondition;
import io.stargate.db.schema.Column;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.SchemalessErrorHandlingStrategy;
import io.stargate.web.docsapi.exception.SchemalessRequestException;
import io.stargate.web.docsapi.service.filter.FilterCondition;
import io.stargate.web.docsapi.service.filter.FilterOp;
import io.stargate.web.docsapi.service.filter.ListFilterCondition;
import io.stargate.web.docsapi.service.filter.SingleFilterCondition;
import io.stargate.web.resources.Db;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
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

public class SchemalessService {
  private static final Logger logger = LoggerFactory.getLogger(SchemalessService.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  /*
   * Converts a JSON path string (e.g. "$.a.b.c[0]") into a JSON path string
   * that only uses square brackets to denote pathing (e.g. "$['a']['b']['c'][0]".
   * This is to allow escaping of certain characters, such as space, $, and @.
   */
  private String convertToBracketedPath(String path) {
    String[] parts = path.split("\\.");
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
    String padded = "";
    for (int pad = 0; pad < 6 - value.length(); pad++) {
      padded += "0";
    }
    return padded + value;
  }

  private String convertArrayPath(String path) {
    if (path.startsWith("[") && path.endsWith("]")) {
      String innerPath = path.substring(1, path.length() - 1);
      int idx = Integer.parseInt(innerPath);
      if (idx > DocumentDB.MAX_ARRAY_LENGTH - 1) {
        throw new SchemalessRequestException(
            String.format("Max array length of %s exceeded.", DocumentDB.MAX_ARRAY_LENGTH));
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
   * @param payload a JSON object
   * @param now The write time to use for this object
   * @param patching If this payload meant to be part of a PATCH request (this causes a small amount
   *     of extra validation if true)
   * @return The full bind variable list for the subsequent inserts, and all first-level keys, as an
   *     ImmutablePair.
   */
  private ImmutablePair<List<Object[]>, List<String>> shredPayload(
      JsonSurfer surfer,
      DocumentDB db,
      List<String> path,
      String key,
      String payload,
      long now,
      boolean patching) {
    List<Object[]> bindVariableList = new ArrayList<>();
    List<String> firstLevelKeys = new ArrayList<>();
    surfer
        .configBuilder()
        .bind(
            "$..*",
            (v, parsingContext) -> {
              String fieldName = parsingContext.getCurrentFieldName();
              if (fieldName != null && (DocumentDB.containsIllegalChars(fieldName))) {
                throw new SchemalessRequestException(
                    String.format(
                        "The characters %s are not permitted in JSON field names, invalid field %s",
                        DocumentDB.getForbiddenCharactersMessage(), fieldName));
              }

              if (v instanceof JsonPrimitive
                  || v instanceof JsonNull
                  || isEmptyObject(v)
                  || isEmptyArray(v)) {
                JsonPath p =
                    JsonPathCompiler.compile(convertToBracketedPath(parsingContext.getJsonPath()));
                int i = path.size();
                Map<String, Object> bindMap = db.newBindMap(path, now);

                bindMap.put("key", key);

                Iterator<PathOperator> it = p.iterator();
                String leaf = null;
                while (it.hasNext()) {
                  if (i >= DocumentDB.MAX_DEPTH) {
                    throw new SchemalessRequestException(
                        String.format("Max depth of %s exceeded", DocumentDB.MAX_DEPTH));
                  }

                  PathOperator op = it.next();
                  String pv = op.toString();

                  if (pv.equals("$")) continue;

                  // pv always starts with a square brace because of the above conversion
                  String innerPath = pv.substring(1, pv.length() - 1);
                  boolean isArrayElement = op.getType() == PathOperator.Type.ARRAY;
                  if (isArrayElement) {
                    if (i == path.size() && patching) {
                      throw new SchemalessRequestException(
                          "A patch operation must be done with a JSON object, not an array.");
                    }

                    int idx = Integer.parseInt(innerPath);
                    if (idx > DocumentDB.MAX_ARRAY_LENGTH - 1) {
                      throw new SchemalessRequestException(
                          String.format(
                              "Max array length of %s exceeded.", DocumentDB.MAX_ARRAY_LENGTH));
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
                    bindMap.put("bool_value", value.getAsBoolean());
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
        .withErrorStrategy(new SchemalessErrorHandlingStrategy())
        .buildAndSurf(payload);
    return ImmutablePair.of(bindVariableList, firstLevelKeys);
  }

  /*
   * Unlike putAtPath, which handles all other updates, putAtRoot only handles root document PUTs.
   * The PUT only succeeds if a document with the provided @param id does not yet exist.
   * @return a boolean detailing success
   */
  public boolean putAtRoot(
      String authToken, String keyspace, String collection, String id, String payload, Db dbFactory)
      throws UnauthorizedException {
    DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);

    JsonSurfer surfer = JsonSurferGson.INSTANCE;

    db.maybeCreateTable(keyspace, collection);

    long now = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());

    List<Object[]> bindVariableList =
        shredPayload(surfer, db, new ArrayList<>(), id, payload, now, false).left;

    if (bindVariableList.size() == 0) {
      throw new SchemalessRequestException(
          "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: "
              + payload
              + "\nHint: update the parent path with a defined object instead.");
    }

    logger.debug("Bind {}", bindVariableList.size());
    return db.insertBatchIfNotExists(keyspace, collection, id, bindVariableList);
  }

  public void putAtPath(
      String authToken,
      String keyspace,
      String collection,
      String id,
      String payload,
      List<PathSegment> path,
      boolean patching,
      Db dbFactory)
      throws UnauthorizedException {
    DocumentDB db = dbFactory.getDocDataStoreForToken(authToken);

    JsonSurfer surfer = JsonSurferGson.INSTANCE;

    db.maybeCreateTable(keyspace, collection);

    // Left-pad the path segments that represent arrays
    List<String> convertedPath = new ArrayList<>(path.size());
    for (PathSegment pathSegment : path) {
      String pathStr = pathSegment.getPath();
      convertedPath.add(convertArrayPath(pathStr));
    }

    Long now = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    ImmutablePair<List<Object[]>, List<String>> shreddingResults =
        shredPayload(surfer, db, convertedPath, id, payload, now, patching);

    List<Object[]> bindVariableList = shreddingResults.left;
    List<String> firstLevelKeys = shreddingResults.right;

    if (bindVariableList.size() == 0) {
      throw new SchemalessRequestException(
          "Updating a key with just a JSON primitive, empty object, or empty array is not allowed. Found: "
              + payload
              + "\nHint: update the parent path with a defined object instead.");
    }

    logger.debug("Bind {}", bindVariableList.size());

    if (patching) {
      db.deletePatchedPathsThenInsertBatch(
          keyspace, collection, id, bindVariableList, convertedPath, firstLevelKeys, now);
    } else {
      db.deleteThenInsertBatch(keyspace, collection, id, bindVariableList, convertedPath, now);
    }
  }

  public JsonNode getJsonAtPath(
      DocumentDB db, String keyspace, String collection, String id, List<PathSegment> path)
      throws ExecutionException, InterruptedException {
    List<Where<Object>> predicates = new ArrayList<>();
    predicates.add(
        ImmutableWhereCondition.builder()
            .column("key")
            .predicate(WhereCondition.Predicate.Eq)
            .value(id)
            .build());

    StringBuilder pathStr = new StringBuilder();
    for (int i = 0; i < path.size(); i++) {
      String pathSegment = path.get(i).getPath();
      String convertedPath = convertArrayPath(pathSegment);
      predicates.add(
          ImmutableWhereCondition.builder()
              .column("p" + i)
              .predicate(WhereCondition.Predicate.Eq)
              .value(convertedPath)
              .build());

      if (!pathSegment.equals(convertedPath)) {
        pathStr.append("/").append(pathSegment, 1, pathSegment.length() - 1);
      } else {
        pathStr.append("/").append(pathSegment);
      }
    }

    ResultSet r = db.executeSelect(keyspace, collection, predicates);
    List<Row> rows = r.rows();

    if (rows.size() == 0) return null;
    ImmutablePair<JsonNode, Map<String, List<JsonNode>>> result = convertToJsonDoc(rows, false);
    if (!result.right.isEmpty()) {
      logger.info(String.format("Deleting %d dead leaves", result.right.size()));
      db.deleteDeadLeaves(keyspace, collection, id, result.right);
    }
    JsonNode node = result.left.at(pathStr.toString());
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
        throw new SchemalessRequestException(
            String.format(
                "Value entry for field %s, operation %s was expecting a value or `null`",
                fieldName, op));
      }
    } else if (filterOp == FilterOp.EXISTS) {
      if (!value.isBoolean() || !value.asBoolean()) {
        throw new SchemalessRequestException(
            String.format("%s only supports the value `true`", op));
      }
    } else if (filterOp == FilterOp.GT
        || filterOp == FilterOp.GTE
        || filterOp == FilterOp.LT
        || filterOp == FilterOp.LTE
        || filterOp == FilterOp.EQ) {
      if (value.isArray() || value.isObject() || value.isNull()) {
        throw new SchemalessRequestException(
            String.format(
                "Value entry for field %s, operation %s was expecting a non-null value",
                fieldName, op));
      }
    } else if (filterOp == FilterOp.IN || filterOp == FilterOp.NIN) {
      if (!value.isArray()) {
        throw new SchemalessRequestException(
            String.format(
                "Value entry for field %s, operation %s was expecting an array", fieldName, op));
      }
    } else {
      throw new IllegalStateException(String.format("Unknown FilterOp value %s", filterOp));
    }
  }

  public List<String> convertToSelectionList(JsonNode fieldsJson) {
    if (!fieldsJson.isArray()) {
      throw new SchemalessRequestException(
          String.format("`fields` must be a JSON array, found %s", fieldsJson));
    }

    List<String> res = new ArrayList<>();
    for (int i = 0; i < fieldsJson.size(); i++) {
      JsonNode value = fieldsJson.get(i);
      if (!value.isTextual()) {
        throw new SchemalessRequestException(
            String.format("Each field must be a string, found %s", value));
      }
      res.add(value.asText());
    }

    return res;
  }

  public List<FilterCondition> convertToFilterOps(
      List<PathSegment> prependedPath, JsonNode filterJson) {
    List<FilterCondition> conditions = new ArrayList<>();

    if (!filterJson.isObject()) {
      throw new SchemalessRequestException("Search was expecting a JSON object as input.");
    }
    ObjectNode input = (ObjectNode) filterJson;
    Iterator<String> fields = input.fieldNames();
    while (fields.hasNext()) {
      String fieldName = fields.next();
      if (fieldName.isEmpty()) {
        throw new SchemalessRequestException(
            "The field(s) you are searching for can't be the empty string!");
      }
      String[] fieldNamePath = fieldName.split("\\.");
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
        throw new SchemalessRequestException(
            String.format(
                "Search entry for field %s was expecting a JSON object as input.", fieldName));
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
      DocumentDB db, String keyspace, String collection, String id, List<PathSegment> path) {
    List<String> convertedPath = new ArrayList<>(path.size());
    for (PathSegment pathSegment : path) {
      String pathStr = pathSegment.getPath();
      convertedPath.add(convertArrayPath(pathStr));
    }
    Long now = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    db.delete(keyspace, collection, id, convertedPath, now);
  }

  public JsonNode searchDocuments(
      DocumentDB db,
      String keyspace,
      String collection,
      String documentKey,
      List<FilterCondition> filters,
      List<PathSegment> path,
      Boolean recurse)
      throws ExecutionException, InterruptedException {
    StringBuilder pathStr = new StringBuilder();

    List<String> pathSegmentValues =
        path.stream().map(PathSegment::getPath).collect(Collectors.toList());
    List<Row> rows =
        searchRows(
                keyspace,
                collection,
                db,
                filters,
                new ArrayList<>(),
                pathSegmentValues,
                recurse,
                documentKey)
            .left;

    if (rows.size() == 0) return null;

    ObjectNode docsResult = mapper.createObjectNode();
    Map<String, List<Row>> rowsByDoc = new HashMap<>();
    addRowsToMap(rowsByDoc, rows);

    for (Map.Entry<String, List<Row>> entry : rowsByDoc.entrySet()) {
      ImmutablePair<JsonNode, Map<String, List<JsonNode>>> result =
          convertToJsonDoc(entry.getValue(), true);
      if (!result.right.isEmpty()) {
        logger.info(String.format("Deleting %d dead leaves", result.right.size()));
        db.deleteDeadLeaves(keyspace, collection, entry.getKey(), result.right);
      }
      JsonNode node = result.left.requiredAt(pathStr.toString());
      docsResult.set(entry.getKey(), node);
    }

    if (docsResult.isMissingNode() || (docsResult.isObject() && docsResult.isEmpty())) {
      return null;
    }

    return docsResult;
  }

  public ImmutablePair<JsonNode, ByteBuffer> searchDocumentsV2(
      DocumentDB db,
      String keyspace,
      String collection,
      List<FilterCondition> filters,
      List<String> fields,
      String documentId)
      throws ExecutionException, InterruptedException {
    FilterCondition first = filters.get(0);
    List<String> path = first.getPath();

    ImmutablePair<List<Row>, ByteBuffer> searchResult =
        searchRows(keyspace, collection, db, filters, fields, path, false, documentId);
    List<Row> rows = searchResult.left;
    ByteBuffer pageState = searchResult.right;
    if (rows.size() == 0) return null;

    JsonNode docsResult = mapper.createObjectNode();

    if (fields.isEmpty()) {
      Map<String, List<Row>> rowsByDoc = new HashMap<>();

      for (Row row : rows) {
        String key = row.getString("key");
        List<Row> rowsAtKey = rowsByDoc.getOrDefault(key, new ArrayList<>());
        rowsAtKey.add(row);
        rowsByDoc.put(key, rowsAtKey);
      }

      for (Map.Entry<String, List<Row>> entry : rowsByDoc.entrySet()) {
        ArrayNode ref = mapper.createArrayNode();
        if (documentId == null) {
          ((ObjectNode) docsResult).set(entry.getKey(), ref);
        } else {
          docsResult = ref;
        }
        List<Row> docRows = entry.getValue();
        for (Row row : docRows) {
          List<Row> wrapped = new ArrayList<>(1);
          wrapped.add(row);
          JsonNode jsonDoc = convertToJsonDoc(wrapped, true).left;
          ref.add(jsonDoc);
        }
      }
    } else {
      if (documentId != null) {
        docsResult = mapper.createArrayNode();
      }
      for (List<Row> chunk : Lists.partition(rows, fields.size())) {
        String key = chunk.get(0).getString("key");
        if (!docsResult.has(key) && documentId == null) {
          ((ObjectNode) docsResult).set(key, mapper.createArrayNode());
        }

        List<Row> nonNull = chunk.stream().filter(x -> x != null).collect(Collectors.toList());
        JsonNode jsonDoc = convertToJsonDoc(nonNull, true).left;

        if (documentId == null) {
          ((ArrayNode) docsResult.get(key)).add(jsonDoc);
        } else {
          ((ArrayNode) docsResult).add(jsonDoc);
        }
      }
    }

    if (docsResult.isMissingNode() || (docsResult.isObject() && docsResult.isEmpty())) {
      return null;
    }

    return ImmutablePair.of(docsResult, pageState);
  }

  private void addRowsToMap(Map<String, List<Row>> rowsByDoc, List<Row> rows) {
    for (Row row : rows) {
      String key = row.getString("key");
      List<Row> rowsAtKey = rowsByDoc.getOrDefault(key, new ArrayList<>());
      rowsAtKey.add(row);
      rowsByDoc.put(key, rowsAtKey);
    }
  }

  private void updateExistenceForMap(
      Map<String, Boolean> existsByDoc,
      Map<String, Integer> countsByDoc,
      List<Row> rows,
      List<FilterCondition> filters) {
    List<Row> filteredRows = applyInMemoryFilters(rows, filters, 1);
    for (Row row : filteredRows) {
      String key = row.getString("key");
      if (!existsByDoc.getOrDefault(key, false)) {
        existsByDoc.put(key, true);
      }
    }

    for (Row row : rows) {
      String key = row.getString("key");
      int value = countsByDoc.getOrDefault(key, 0);
      countsByDoc.put(key, value + 1);
    }
  }

  /**
   * This method gets all the rows for @param limit documents, by fetching result sets sequentially
   * and stringing them together. This is NOT expected to perform well for large documents.
   */
  public ImmutablePair<JsonNode, ByteBuffer> getFullDocuments(
      Db dbFactory,
      DocumentDB db,
      String authToken,
      String keyspace,
      String collection,
      List<String> fields,
      ByteBuffer initialPagingState,
      int pageSize,
      int limit)
      throws ExecutionException, InterruptedException, UnauthorizedException {
    ObjectNode docsResult = mapper.createObjectNode();
    LinkedHashMap<String, List<Row>> rowsByDoc = new LinkedHashMap<>();

    ImmutablePair<List<Row>, ByteBuffer> page;
    do {
      page =
          searchRows(
              keyspace,
              collection,
              db,
              new ArrayList<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              false,
              null);
      addRowsToMap(rowsByDoc, page.left);
      db = dbFactory.getDocDataStoreForToken(authToken, pageSize, page.right);
    } while (rowsByDoc.keySet().size() <= limit && page.right != null);

    // Either we've reached the end of all rows in the collection, or we have enough rows
    // in memory to build the final result.
    Set<String> docNames = rowsByDoc.keySet();
    if (docNames.size() > limit) {
      int totalSize = 0;
      Iterator<Map.Entry<String, List<Row>>> iter = rowsByDoc.entrySet().iterator();
      for (int i = 0; i < limit; i++) {
        Map.Entry<String, List<Row>> e = iter.next();
        totalSize += e.getValue().size();
        List<Row> rows = new ArrayList<>();
        for (Row row : e.getValue()) {
          if (fields.isEmpty() || fields.contains(row.getString("p0"))) rows.add(row);
        }
        docsResult.set(e.getKey(), convertToJsonDoc(rows, false).left);
      }
      db = dbFactory.getDocDataStoreForToken(authToken, totalSize, initialPagingState);
      ByteBuffer finalPagingState =
          searchRows(
                  keyspace,
                  collection,
                  db,
                  new ArrayList<>(),
                  new ArrayList<>(),
                  new ArrayList<>(),
                  false,
                  null)
              .right;
      return ImmutablePair.of(docsResult, finalPagingState);
    } else {
      Iterator<Map.Entry<String, List<Row>>> iter = rowsByDoc.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, List<Row>> e = iter.next();
        List<Row> rows = new ArrayList<>();
        for (Row row : e.getValue()) {
          if (fields.isEmpty() || fields.contains(row.getString("p0"))) rows.add(row);
        }
        docsResult.set(e.getKey(), convertToJsonDoc(rows, false).left);
      }
      return ImmutablePair.of(docsResult, null);
    }
  }

  /**
   * This method gets all the rows for @param limit documents, by fetching result sets sequentially
   * and stringing them together. After getting all the data out, it will pare down the document to
   * just the relevant result set, while maintaining page state. This is expected to be even more
   * intensive than getFullDocuments.
   */
  public ImmutablePair<JsonNode, ByteBuffer> getFullDocumentsFiltered(
      Db dbFactory,
      DocumentDB db,
      String authToken,
      String keyspace,
      String collection,
      List<FilterCondition> filters,
      List<String> fields,
      ByteBuffer initialPagingState,
      int pageSize,
      int limit)
      throws ExecutionException, InterruptedException, UnauthorizedException {
    ObjectNode docsResult = mapper.createObjectNode();
    LinkedHashMap<String, Boolean> existsByDoc = new LinkedHashMap<>();
    LinkedHashMap<String, Integer> countsByDoc = new LinkedHashMap<>();

    ImmutablePair<List<Row>, ByteBuffer> page;
    do {
      page =
          searchRows(
              keyspace,
              collection,
              db,
              new ArrayList<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              false,
              null);
      updateExistenceForMap(existsByDoc, countsByDoc, page.left, filters);
      db = dbFactory.getDocDataStoreForToken(authToken, pageSize, page.right);
    } while (existsByDoc.keySet().size() <= limit && page.right != null);

    // Either we've reached the end of all rows in the collection, or we have enough rows
    // in memory to build the final result.
    Set<String> docNames = existsByDoc.keySet();
    ByteBuffer finalPagingState;
    if (docNames.size() > limit) {
      int totalSize = 0;
      docNames = new HashSet<>();
      Iterator<Map.Entry<String, Integer>> iter = countsByDoc.entrySet().iterator();
      int i = 0;
      while (i < limit) {
        Map.Entry<String, Integer> e = iter.next();
        totalSize += e.getValue();
        if (existsByDoc.containsKey(e.getKey())) {
          docNames.add(e.getKey());
          i++;
        }
      }
      db = dbFactory.getDocDataStoreForToken(authToken, totalSize, initialPagingState);
      finalPagingState =
          searchRows(
                  keyspace,
                  collection,
                  db,
                  new ArrayList<>(),
                  new ArrayList<>(),
                  new ArrayList<>(),
                  false,
                  null)
              .right;
    } else {
      finalPagingState = null;
    }

    List<Where<Object>> predicate =
        ImmutableList.of(
            ImmutableWhereCondition.builder()
                .column("key")
                .predicate(WhereCondition.Predicate.In)
                .value(new ArrayList<>(docNames))
                .build());

    db = dbFactory.getDocDataStoreForToken(authToken);
    List<Row> rows = db.executeSelect(keyspace, collection, predicate).rows();
    Map<String, List<Row>> rowsByDoc = new HashMap<>();
    for (Row row : rows) {
      String key = row.getString("key");
      List<Row> rowsAtKey = rowsByDoc.getOrDefault(key, new ArrayList<>());
      if (fields.isEmpty() || fields.contains(row.getString("p0"))) rowsAtKey.add(row);
      rowsByDoc.put(key, rowsAtKey);
    }

    for (Map.Entry<String, List<Row>> entry : rowsByDoc.entrySet()) {
      docsResult.set(entry.getKey(), convertToJsonDoc(entry.getValue(), false).left);
    }

    return ImmutablePair.of(docsResult, finalPagingState);
  }

  /**
   * Searches a document collection for particular results. If `fields` is non-empty, queries
   * Cassandra for any data that matches `path` and then does matching of the selection set and
   * filtering in memory.
   *
   * <p>A major restriction: if `fields` is non-empty or `filters` includes a filter that has
   * "limited support" ($nin, $in, $ne), then the result set MUST fit in a single page. A requester
   * could alter `page-size` up to a limit of 1000 to attempt to achieve this.
   *
   * @param keyspace the keyspace (document namespace) where the table lives
   * @param collection the table (document collection)
   * @param db the DB utility
   * @param filters A list of FilterConditions
   * @param fields the fields to return
   * @param path the path in the document that is being searched on
   * @param recurse legacy boolean, only used for v1 of the API
   * @param documentKey filter down to only one document's results
   * @return
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private ImmutablePair<List<Row>, ByteBuffer> searchRows(
      String keyspace,
      String collection,
      DocumentDB db,
      List<FilterCondition> filters,
      List<String> fields,
      List<String> path,
      Boolean recurse,
      String documentKey)
      throws ExecutionException, InterruptedException {
    StringBuilder pathStr = new StringBuilder();
    List<Where<Object>> predicates = new ArrayList<>();

    if (!filters.isEmpty() && fields.isEmpty()) {
      FilterCondition first = filters.get(0);
      predicates.add(
          ImmutableWhereCondition.builder()
              .column("leaf")
              .predicate(WhereCondition.Predicate.Eq)
              .value(first.getField())
              .build());
    }

    boolean manyPathsFound = false;

    int i;
    for (i = 0; i < path.size(); i++) {
      String[] pathSegmentSplit = path.get(i).split(",");
      if (pathSegmentSplit.length == 1) {
        String pathSegment = pathSegmentSplit[0];
        if (pathSegment.equals(DocumentDB.GLOB_VALUE)) {
          manyPathsFound = true;
          predicates.add(
              ImmutableWhereCondition.builder()
                  .column("p" + i)
                  .predicate(WhereCondition.Predicate.Gt)
                  .value("")
                  .build());
        } else {
          String convertedPath = convertArrayPath(pathSegment);
          predicates.add(
              ImmutableWhereCondition.builder()
                  .column("p" + i)
                  .predicate(WhereCondition.Predicate.Eq)
                  .value(convertedPath)
                  .build());
          if (!manyPathsFound) {
            pathStr.append("/").append(pathSegment);
          }
        }
      } else {
        List<String> segmentsList = Arrays.asList(pathSegmentSplit);
        // left pad any array segments to 6 places
        segmentsList =
            segmentsList.stream().map(this::convertArrayPath).collect(Collectors.toList());

        manyPathsFound = true;
        predicates.add(
            ImmutableWhereCondition.builder()
                .column("p" + i)
                .predicate(WhereCondition.Predicate.In)
                .value(segmentsList)
                .build());
      }
    }

    List<FilterCondition> inCassandraFilters = new ArrayList<>();
    List<FilterCondition> inMemoryFilters = filters;

    if (fields.isEmpty()) {
      inCassandraFilters =
          filters.stream()
              .filter(f -> !FilterOp.LIMITED_SUPPORT_FILTERS.contains(f.getFilterOp()))
              .collect(Collectors.toList());
      inMemoryFilters =
          filters.stream()
              .filter(f -> FilterOp.LIMITED_SUPPORT_FILTERS.contains(f.getFilterOp()))
              .collect(Collectors.toList());
    }

    if ((recurse == null || !recurse)
        && path.size() < db.MAX_DEPTH
        && !inCassandraFilters.isEmpty()) {
      predicates.add(
          ImmutableWhereCondition.builder()
              .column("p" + i++)
              .predicate(WhereCondition.Predicate.Eq)
              .value(filters.get(0).getField())
              .build());
    } else if (!path.isEmpty()) {
      predicates.add(
          ImmutableWhereCondition.builder()
              .column("p" + i++)
              .predicate(WhereCondition.Predicate.Gt)
              .value("")
              .build());
    }

    // The rest of the paths must match empty-string
    while (i < db.MAX_DEPTH && !path.isEmpty()) {
      predicates.add(
          ImmutableWhereCondition.builder()
              .column("p" + i++)
              .predicate(WhereCondition.Predicate.Eq)
              .value("")
              .build());
    }

    for (FilterCondition filter : inCassandraFilters) {
      // All fully supported filters are SingleFilterConditions, as of now
      SingleFilterCondition singleFilter = (SingleFilterCondition) filter;
      FilterOp queryOp = singleFilter.getFilterOp();
      String queryValueField = singleFilter.getValueColumnName();
      Object queryValue = singleFilter.getValue();
      if (queryOp != FilterOp.EXISTS) {
        predicates.add(
            ImmutableWhereCondition.builder()
                .column(queryValueField)
                .predicate(queryOp.predicate)
                .value(queryValue)
                .build());
      }
    }

    ResultSet r;

    if (predicates.size() > 0) {
      r = db.executeSelect(keyspace, collection, predicates, true);
    } else {
      r = db.executeSelectAll(keyspace, collection);
    }

    List<Row> rows = r.rows();
    ByteBuffer newState = r.getPagingState();

    if (documentKey != null) {
      rows =
          rows.stream()
              .filter(row -> row.getString("key").equals(documentKey))
              .collect(Collectors.toList());
    }

    if (!inMemoryFilters.isEmpty() && newState != null) {
      throw new SchemalessRequestException(
          "The results as requested must fit in one page, try increasing the `page-size` parameter.");
    }
    rows = filterToSelectionSet(rows, fields, path);
    rows = applyInMemoryFilters(rows, inMemoryFilters, Math.max(fields.size(), 1));

    return ImmutablePair.of(rows, newState);
  }

  private String getParentPathFromRow(Row row) {
    int i = 0;
    StringBuilder s = new StringBuilder();
    boolean end = false;
    s.append(row.getString("key")).append("/");
    while (i < DocumentDB.MAX_DEPTH && !end) {
      String pathSegment = row.getString("p" + i);
      String nextPathSegment = i + 1 < DocumentDB.MAX_DEPTH ? row.getString("p" + (i + 1)) : null;
      end = (nextPathSegment == null || nextPathSegment.equals(""));
      if (!end) {
        s.append(pathSegment).append(".");
        i++;
      }
    }
    return s.toString();
  }

  private List<Row> filterToSelectionSet(
      List<Row> rows, List<String> fieldNames, List<String> requestedPath) {
    if (fieldNames.isEmpty()) {
      return rows;
    }
    // The expectation here is that if N rows match the inMemoryFilters,
    // there will be exactly N * selectionSet.size() rows in this list, and they will
    // be grouped in order of the rows' path. This means adding in "fluff" empty or null rows
    // to round out the size of the list when a field doesn't exist.
    List<Row> normalizedList = new ArrayList<>();
    Set<String> selectionSet = new HashSet<>(fieldNames);
    String currentPath = "";
    int docSize = 0;
    for (Row row : rows) {
      String path = getParentPathFromRow(row);
      if (!currentPath.equals(path)
          && !currentPath.isEmpty()
          && currentPath.split("\\.").length == requestedPath.size()) {
        for (int i = 0; i < selectionSet.size() - docSize; i++) {
          normalizedList.add(null);
        }
        docSize = 0;
      }
      currentPath = path;

      if (selectionSet.contains(row.getString("leaf"))
          && currentPath.split("\\.").length == requestedPath.size()) {
        normalizedList.add(row);
        docSize++;
      }
    }

    if (currentPath.split("\\.").length == requestedPath.size()) {
      for (int i = 0; i < selectionSet.size() - docSize; i++) {
        normalizedList.add(null);
      }
    }

    return normalizedList;
  }

  /**
   * Applies all of the @param inMemoryFilters, conjunctively (using AND).
   *
   * @param rows the result set from cassandra
   * @param inMemoryFilters the filters to apply to `rows`
   * @param fieldsPerDoc The number of rows that make up a single document result
   * @return rows for each doc that match all filters
   */
  private List<Row> applyInMemoryFilters(
      List<Row> rows, List<FilterCondition> inMemoryFilters, int fieldsPerDoc) {
    if (inMemoryFilters.size() == 0) {
      return rows;
    }
    String filterField = inMemoryFilters.get(0).getField();

    return Lists.partition(rows, fieldsPerDoc).stream()
        .filter(
            docChunk -> {
              Optional<Row> fieldRow =
                  docChunk.stream()
                      .filter(
                          r -> r != null && StringUtils.equals(r.getString("leaf"), filterField))
                      .findFirst();
              return fieldRow.isPresent() && allFiltersMatch(fieldRow.get(), inMemoryFilters);
            })
        .flatMap(x -> x.stream())
        .collect(Collectors.toList());
  }

  private boolean allFiltersMatch(Row row, List<FilterCondition> filters) {
    String textValue = row.has("text_value") ? row.getString("text_value") : null;
    Boolean boolValue = row.has("bool_value") ? row.getBoolean("bool_value") : null;
    Double dblValue = row.has("dbl_value") ? row.getDouble("dbl_value") : null;
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
    boolean boolValueEqual = filterCondition.getBooleanValue() == boolValue;
    boolean dblValueEqual =
        filterCondition.getDoubleValue() == dblValue
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

  public ImmutablePair<JsonNode, Map<String, List<JsonNode>>> convertToJsonDoc(
      List<Row> rows, boolean writeAllPathsAsObjects) {
    JsonNode doc = mapper.createObjectNode();
    Map<String, Long> pathWriteTimes = new HashMap<>();
    Map<String, List<JsonNode>> deadLeaves = new HashMap<>();
    if (rows.isEmpty()) {
      return ImmutablePair.of(doc, deadLeaves);
    }
    Column writeTimeCol = Column.reference("writetime(leaf)");

    for (Row row : rows) {
      Long rowWriteTime = row.getLong(writeTimeCol);
      String rowLeaf = row.getString("leaf");
      if (rowLeaf.equals(DocumentDB.ROOT_DOC_MARKER)) {
        continue;
      }

      String leaf = null;
      JsonNode parentRef = null;
      JsonNode ref = doc;

      String parentPath = "$";

      for (int i = 0; i < DocumentDB.MAX_DEPTH - 1; i++) {
        String p = row.getString("p" + i);
        String nextP = row.getString("p" + (i + 1));
        boolean endOfPath = nextP.equals("");
        boolean isArray = p.startsWith("[");
        boolean nextIsArray = nextP.startsWith("[");

        if (isArray) {
          // This removes leading zeros if applicable
          p = "[" + Integer.parseInt(p.substring(1, p.length() - 1)) + "]";
        }

        boolean shouldWrite =
            !pathWriteTimes.containsKey(parentPath)
                || pathWriteTimes.get(parentPath) <= rowWriteTime;

        if (!shouldWrite) {
          markFullPathAsDead(parentPath, p, deadLeaves);
          break;
        }

        if (endOfPath) {
          boolean shouldBeArray = isArray && !ref.isArray() && !writeAllPathsAsObjects;
          if (i == 0 && shouldBeArray) {
            doc = mapper.createArrayNode();
            ref = doc;
            pathWriteTimes.put(parentPath, rowWriteTime);
          } else if (i != 0 && shouldBeArray) {
            markObjectAtPathAsDead(ref, parentPath, deadLeaves);
            ref = changeCurrentNodeToArray(row, parentRef, i);
            pathWriteTimes.put(parentPath, rowWriteTime);
          } else if (i != 0 && !isArray && !ref.isObject()) {
            markArrayAtPathAsDead(ref, parentPath, deadLeaves);
            ref = changeCurrentNodeToObject(row, parentRef, i, writeAllPathsAsObjects);
            pathWriteTimes.put(parentPath, rowWriteTime);
          }
          leaf = p;
          break;
        }

        JsonNode childRef;

        if (isArray && !writeAllPathsAsObjects) {
          if (!ref.isArray()) {
            if (i == 0) {
              doc = mapper.createArrayNode();
              ref = doc;
              pathWriteTimes.put(parentPath, rowWriteTime);
            } else {
              markObjectAtPathAsDead(ref, parentPath, deadLeaves);
              ref = changeCurrentNodeToArray(row, parentRef, i);
              pathWriteTimes.put(parentPath, rowWriteTime);
            }
          }

          int index = Integer.parseInt(p.substring(1, p.length() - 1));

          ArrayNode arrayRef = (ArrayNode) ref;

          int currentSize = arrayRef.size();
          for (int k = currentSize; k < index; k++) arrayRef.addNull();

          if (currentSize <= index) {
            childRef = nextIsArray ? mapper.createArrayNode() : mapper.createObjectNode();
            arrayRef.add(childRef);
          } else {
            childRef = arrayRef.get(index);

            // Replace null from above (out of order)
            if (childRef.isNull()) {
              childRef = nextIsArray ? mapper.createArrayNode() : mapper.createObjectNode();
            }

            arrayRef.set(index, childRef);
          }
          parentRef = ref;
          ref = childRef;
        } else {
          childRef = ref.get(p);
          if (childRef == null) {
            childRef =
                nextIsArray && !writeAllPathsAsObjects
                    ? mapper.createArrayNode()
                    : mapper.createObjectNode();

            if (!ref.isObject()) {
              markArrayAtPathAsDead(ref, parentPath, deadLeaves);
              ref = changeCurrentNodeToObject(row, parentRef, i, writeAllPathsAsObjects);
              pathWriteTimes.put(parentPath, rowWriteTime);
            }

            ((ObjectNode) ref).set(p, childRef);
          }
          parentRef = ref;
          ref = childRef;
        }
        parentPath += "." + p;
      }

      if (leaf == null) {
        continue;
      }

      writeLeafIfNewer(ref, row, leaf, parentPath, pathWriteTimes, rowWriteTime);
    }

    return ImmutablePair.of(doc, deadLeaves);
  }

  private JsonNode changeCurrentNodeToArray(Row row, JsonNode parentRef, int pathIndex) {
    String pbefore = row.getString("p" + (pathIndex - 1));
    JsonNode ref = mapper.createArrayNode();
    if (pbefore.startsWith("[")) {
      int index = Integer.parseInt(pbefore.substring(1, pbefore.length() - 1));
      ((ArrayNode) parentRef).set(index, ref);
    } else {
      ((ObjectNode) parentRef).set(pbefore, ref);
    }

    return ref;
  }

  private JsonNode changeCurrentNodeToObject(
      Row row, JsonNode parentRef, int pathIndex, boolean writeAllPathsAsObjects) {
    String pbefore = row.getString("p" + (pathIndex - 1));
    JsonNode ref = mapper.createObjectNode();
    if (pbefore.startsWith("[") && !writeAllPathsAsObjects) {
      int index = Integer.parseInt(pbefore.substring(1, pbefore.length() - 1));
      ((ArrayNode) parentRef).set(index, ref);
    } else {
      ((ObjectNode) parentRef).set(pbefore, ref);
    }
    return ref;
  }

  private void markFullPathAsDead(
      String parentPath, String currentPath, Map<String, List<JsonNode>> deadLeaves) {
    List<JsonNode> deadLeavesAtPath = deadLeaves.getOrDefault(parentPath, new ArrayList<>());
    if (!deadLeavesAtPath.isEmpty()) {
      ObjectNode node = (ObjectNode) deadLeavesAtPath.get(0);
      node.set(currentPath, NullNode.getInstance());
    } else {
      ObjectNode node = mapper.createObjectNode();
      node.set(currentPath, NullNode.getInstance());
      deadLeavesAtPath.add(node);
    }
    deadLeaves.put(parentPath, deadLeavesAtPath);
  }

  private void markObjectAtPathAsDead(
      JsonNode ref, String parentPath, Map<String, List<JsonNode>> deadLeaves) {
    List<JsonNode> deadLeavesAtPath = deadLeaves.getOrDefault(parentPath, new ArrayList<>());
    if (!ref.isObject()) {
      ObjectNode node = mapper.createObjectNode();
      node.set("", ref);
      deadLeavesAtPath.add(node);
    } else {
      deadLeavesAtPath.add(ref);
    }
    deadLeaves.put(parentPath, deadLeavesAtPath);
  }

  private void markArrayAtPathAsDead(
      JsonNode ref, String parentPath, Map<String, List<JsonNode>> deadLeaves) {
    List<JsonNode> deadLeavesAtPath = deadLeaves.getOrDefault(parentPath, new ArrayList<>());
    if (!ref.isArray()) {
      ObjectNode node = mapper.createObjectNode();
      node.set("", ref);
      deadLeavesAtPath.add(node);
    } else {
      deadLeavesAtPath.add(ref);
    }
    deadLeaves.put(parentPath, deadLeavesAtPath);
  }

  private void writeLeafIfNewer(
      JsonNode ref,
      Row row,
      String leaf,
      String parentPath,
      Map<String, Long> pathWriteTimes,
      Long rowWriteTime) {
    JsonNode n = NullNode.getInstance();

    if (row.has("text_value")) {
      String value = row.getString("text_value");
      if (value.equals(DocumentDB.EMPTY_OBJECT_MARKER)) {
        n = mapper.createObjectNode();
      } else if (value.equals(DocumentDB.EMPTY_ARRAY_MARKER)) {
        n = mapper.createArrayNode();
      } else {
        n = new TextNode(value);
      }
    } else if (row.has("bool_value")) {
      n = BooleanNode.valueOf(row.getBoolean("bool_value"));
    } else if (row.has("dbl_value")) {
      // If not a fraction represent as a long to the user
      // This lets us handle queries of doubles and longs without
      // splitting them into separate columns
      Double dv = row.getDouble("dbl_value");
      Long lv = dv.longValue();
      if (lv.doubleValue() == dv) n = new LongNode(lv);
      else n = new DoubleNode(dv);
    }
    if (ref == null)
      throw new RuntimeException("Missing path @" + leaf + " v=" + n + " row=" + row.toString());

    boolean shouldWrite =
        !pathWriteTimes.containsKey(parentPath + "." + leaf)
            || pathWriteTimes.get(parentPath + "." + leaf) <= rowWriteTime;
    if (shouldWrite) {
      if (ref.isObject()) {
        ((ObjectNode) ref).set(leaf, n);
      } else if (ref.isArray()) {
        if (!leaf.startsWith("["))
          throw new RuntimeException("Trying to write object to array " + leaf);

        ArrayNode arrayRef = (ArrayNode) ref;
        int index = Integer.parseInt(leaf.substring(1, leaf.length() - 1));

        int currentSize = arrayRef.size();
        for (int k = currentSize; k < index; k++) arrayRef.addNull();

        if (currentSize <= index) {
          arrayRef.add(n);
        } else if (!arrayRef.hasNonNull(index)) {
          arrayRef.set(index, n);
        }
      } else {
        throw new IllegalStateException("Invalid document state: " + ref);
      }
      pathWriteTimes.put(parentPath + "." + leaf, rowWriteTime);
    }
  }
}
