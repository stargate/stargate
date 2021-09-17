package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.base.Splitter;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.exception.RuntimeExceptionPassHandlingStrategy;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.path.JsonPath;
import org.jsfr.json.path.PathOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service in charge of turning a document into its "shredded" form, which is representing the
 * document as a single row per value in the document. Contains utilities for converting documents
 * into lists of bound parameters for writing to the underlying persistence.
 */
public class DocsShredder {
  private static final Logger logger = LoggerFactory.getLogger(DocsApiUtils.class);
  private final Splitter FORM_SPLITTER = Splitter.on('&');
  private final Splitter PAIR_SPLITTER = Splitter.on('=');

  private DocsApiConfiguration docsApiConfiguration;

  @Inject
  public DocsShredder(DocsApiConfiguration docsApiConfiguration) {
    this.docsApiConfiguration = docsApiConfiguration;
  }

  private Object convertToBackendBooleanValue(boolean value, boolean numericBooleans) {
    if (numericBooleans) {
      return value ? 1 : 0;
    }
    return value;
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
  public ImmutablePair<List<Object[]>, List<String>> shredPayload(
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

  public ImmutablePair<List<Object[]>, List<String>> shredJson(
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
              (v0, parsingContext) -> {
                final JsonNode v = (JsonNode) v0;
                String fieldName = parsingContext.getCurrentFieldName();
                if (fieldName != null && DocsApiUtils.containsIllegalSequences(fieldName)) {
                  String msg =
                      String.format(
                          "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field %s",
                          fieldName);
                  throw new ErrorCodeRuntimeException(
                      ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME, msg);
                }

                if (v.isValueNode() // scalar or explicit null
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

                  if (v.isValueNode() && !v.isNull()) {
                    ValueNode value = (ValueNode) v;

                    if (value.isNumber()) {
                      bindMap.put("dbl_value", value.asDouble());
                      bindMap.put("bool_value", null);
                      bindMap.put("text_value", null);
                    } else if (value.isBoolean()) {
                      bindMap.put("dbl_value", null);
                      bindMap.put(
                          "bool_value",
                          convertToBackendBooleanValue(
                              value.asBoolean(), db.treatBooleansAsNumeric()));
                      bindMap.put("text_value", null);
                    } else {
                      bindMap.put("dbl_value", null);
                      bindMap.put("bool_value", null);
                      bindMap.put("text_value", value.asText());
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
      String[] fieldNames = DocsApiUtils.PERIOD_PATTERN.split(fullyQualifiedField);

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

  private boolean isEmptyObject(JsonNode v) {
    return v.isObject() && v.isEmpty();
  }

  private boolean isEmptyArray(JsonNode v) {
    return v.isArray() && v.isEmpty();
  }
}
