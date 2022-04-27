package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.base.Splitter;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.exception.RuntimeExceptionPassHandlingStrategy;
import io.stargate.web.docsapi.exception.UncheckedJacksonException;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.ParsingContext;
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

  private final DocsApiConfiguration config;

  @Inject
  public DocsShredder(DocsApiConfiguration config) {
    this.config = config;
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
   * @param path The path within the document that is being written to. If empty, writes to the root
   *     of the document.
   * @param key The name of the document that will be written
   * @param payload a JSON object, or a URL-encoded form with the relevant data in it
   * @param patching If this payload meant to be part of a PATCH request (this causes a small amount
   *     of extra validation if true)
   * @param numericBooleans - whether booleans are being represented as 0 and 1 in this context
   * @param isJson if the request had a content type of application/json, else it will be
   *     interpreted as a URL encoded form (deprecated)
   * @return The full bind variable list for the subsequent inserts, and all first-level keys, as an
   *     ImmutablePair.
   */
  public ImmutablePair<List<Object[]>, List<String>> shredPayload(
      JsonSurfer surfer,
      List<String> path,
      String key,
      String payload,
      boolean patching,
      boolean numericBooleans,
      boolean isJson) {
    String trimmed = payload.trim();
    if (isJson) {
      return shredJson(surfer, path, key, trimmed, patching, numericBooleans);
    } else {
      // This is a deprecated path, and is un-documented
      // TODO: delete this after no testing relies on it
      return shredForm(path, key, trimmed, patching, numericBooleans);
    }
  }

  /**
   * Turns the payload (a valid JSON string) into a list of bound parameters to write to the
   * underlying persistence. Also returns a list of all "top-level" keys in the form, for the
   * purposes of merging key sets in the event of a PATCH operation. The returned information from
   * this method can be used to write to the underlying persistence using DocumentDB's bound
   * statements.
   *
   * @param surfer - a JsonSurfer instance
   * @param path - a base path, which affects the final bound parameters if present
   * @param key - the ID of the document
   * @param jsonPayload - the payload
   * @param patching - whether we are using this shredding to patch (used only for error checking)
   * @param numericBooleans - whether booleans are being represented as 0 and 1 in this context
   * @return a Pair with a bound parameter list, and a list of top-level keys in the JSON
   */
  public ImmutablePair<List<Object[]>, List<String>> shredJson(
      JsonSurfer surfer,
      List<String> path,
      String key,
      String jsonPayload,
      boolean patching,
      boolean numericBooleans) {
    List<Object[]> bindVariableList = new ArrayList<>();
    List<String> firstLevelKeys = new ArrayList<>();
    try {
      surfer
          .configBuilder()
          .bind(
              "$..*",
              (v0, parsingContext) -> {
                final JsonNode value = (JsonNode) v0;
                String fieldName = parsingContext.getCurrentFieldName();
                if (fieldName != null && DocsApiUtils.containsIllegalSequences(fieldName)) {
                  String msg =
                      String.format(
                          "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field %s",
                          fieldName);
                  throw new ErrorCodeRuntimeException(
                      ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME, msg);
                }
                if (value.isValueNode() // scalar or explicit null
                    || isEmptyObject(value)
                    || isEmptyArray(value)) {
                  ImmutablePair<Map<String, Object>, List<String>> result =
                      convertJsonNodeToBoundVariables(
                          value, parsingContext, path, key, patching, numericBooleans);
                  Map<String, Object> boundVariables = result.left;
                  firstLevelKeys.addAll(result.right);
                  bindVariableList.add(boundVariables.values().toArray());
                }
              })
          .withErrorStrategy(new RuntimeExceptionPassHandlingStrategy())
          .buildAndSurf(jsonPayload);
      return ImmutablePair.of(bindVariableList, firstLevelKeys);
    } catch (ErrorCodeRuntimeException e) {
      throw e;
    } catch (UncheckedJacksonException e) {
      Throwable je = e.getCause();
      // Since we know it's a regular kind of invalid JSON (and not catastrophic
      // processing problem), only warn and no stack trace needed.
      logger.warn(
          "Invalid JSON payload encountered during JSON read ({}): {}",
          je.getClass().getName(),
          je.getMessage());
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE,
          "Malformed JSON object found during read: " + je,
          je);
    } catch (RuntimeException e) {
      // We don't actually know it IS malformed JSON (perhaps we got NPE?) but this is what
      // has been reported so far so keep consistent. And log the stack trace.
      logger.error("Error occurred during JSON read: " + e, e);
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE,
          "Malformed JSON object found during read: " + e,
          e);
    }
  }

  /**
   * Takes a JsonNode's value and its parse context and turns it into the bind parameters for a
   * single row in the underlying persistence. If @param path is provided, then the row will be
   * prefixed with that path.
   *
   * @param jsonValue the JSON value
   * @param parsingContext
   * @param path the path which is always put at the start of the path columns
   * @param key the document's ID
   * @param patching - whether we are using this shredding to patch (used only for error checking)
   * @param numericBooleans - whether booleans are being represented as 0 and 1 in this context
   * @return A Pair containing the binding parameter Map and a List of all top-level keys in the
   *     JSON (used for patching)
   */
  private ImmutablePair<Map<String, Object>, List<String>> convertJsonNodeToBoundVariables(
      JsonNode jsonValue,
      ParsingContext parsingContext,
      List<String> path,
      String key,
      boolean patching,
      boolean numericBooleans) {
    String bracketedPath = DocsApiUtils.convertJsonToBracketedPath(parsingContext.getJsonPath());
    Map<String, Object> bindMap = DocsApiUtils.newBindMap(path, config.getMaxDepth());

    bindMap.put("key", key);
    String leaf = null;
    int i = path.size();

    String[] unboundPaths = new String[config.getMaxDepth()];
    List<String> firstLevelKeys = new ArrayList<>();
    String[] bracketedPathArray = DocsApiUtils.bracketedPathAsArray(bracketedPath);
    for (int j = 0; j < bracketedPathArray.length; j++) {
      if (i >= config.getMaxDepth()) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
      }

      String pathValue = bracketedPathArray[j];
      boolean isArrayValue = pathValue.charAt(0) != '\'';

      // pathValue always starts and ends with a square brace because of
      // DocsApiUtils#convertJsonToBracketedPath
      String innerPath = pathValue;
      if (!isArrayValue) {
        // Remove the single quotes and convert escaped characters
        innerPath = DocsApiUtils.convertEscapedCharacters(pathValue);
        innerPath = innerPath.substring(1, innerPath.length() - 1);
      }
      if (isPatchingWithArrayValue(i, path.size(), isArrayValue, patching)) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PATCH_ARRAY_NOT_ACCEPTED);
      }
      if (isAtTopLevel(i, path.size())) {
        firstLevelKeys.add(innerPath);
      }
      String convertedPath = convertPathValueForArrays(innerPath, isArrayValue);
      unboundPaths[i++] = convertedPath;
      leaf = convertedPath;
    }

    bindMap = addAllToBindMap(bindMap, unboundPaths, jsonValue, leaf, numericBooleans);
    return new ImmutablePair<>(bindMap, firstLevelKeys);
  }

  /**
   * Constructs a bound parameter Map using shredded paths, a JSON value, and a leaf value.
   *
   * @param bindMap the bindMap which already has a prepended path bound to it
   * @param unboundPaths the paths which are not yet bound
   * @param jsonValue the JSON value
   * @param leaf the leaf of the path
   * @param treatBooleansAsNumeric whether booleans are represented by 0 and 1 in this context
   * @return A fully populated bind map
   */
  private Map<String, Object> addAllToBindMap(
      Map<String, Object> bindMap,
      String[] unboundPaths,
      JsonNode jsonValue,
      String leaf,
      boolean treatBooleansAsNumeric) {
    bindMap = addUnboundPaths(bindMap, unboundPaths);
    bindMap.put(DocsApiConstants.LEAF_COLUMN_NAME, leaf);

    if (jsonValue.isValueNode() && !jsonValue.isNull()) {
      ValueNode value = (ValueNode) jsonValue;

      if (value.isNumber()) {
        bindMap.put(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME, value.asDouble());
      } else if (value.isBoolean()) {
        bindMap.put(
            DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME,
            convertToBackendBooleanValue(value.asBoolean(), treatBooleansAsNumeric));
      } else {
        bindMap.put(DocsApiConstants.STRING_VALUE_COLUMN_NAME, value.asText());
      }
    } else if (isEmptyObject(jsonValue)) {
      bindMap.put(DocsApiConstants.STRING_VALUE_COLUMN_NAME, DocsApiConstants.EMPTY_OBJECT_MARKER);
    } else if (isEmptyArray(jsonValue)) {
      bindMap.put(DocsApiConstants.STRING_VALUE_COLUMN_NAME, DocsApiConstants.EMPTY_ARRAY_MARKER);
    }
    return bindMap;
  }

  /**
   * Adds path bindings to the bindMap, starting at the first index in the unboundPaths that is
   * non-null.
   *
   * @param bindMap
   * @param unboundPaths
   * @return a bind map with all bound paths
   */
  private Map<String, Object> addUnboundPaths(Map<String, Object> bindMap, String[] unboundPaths) {
    for (int i = 0; i < unboundPaths.length; i++) {
      String unboundPath = unboundPaths[i];
      if (unboundPath != null) {
        bindMap.put("p" + i, unboundPath);
      }
    }
    return bindMap;
  }

  private boolean isPatchingWithArrayValue(
      int index, int pathSize, boolean isArrayValue, boolean patching) {
    return isAtTopLevel(index, pathSize) && isArrayValue && patching;
  }

  private boolean isAtTopLevel(int index, int pathSize) {
    return index == pathSize;
  }

  /**
   * Standardizes a pathSegment if it is part of an array by left-padding it. If it is not part of
   * an array, this function effectively is a no-op.
   *
   * @param pathSegment the path segment
   * @param isArrayValue If the path's value represents an array
   * @return a converted pathSegment
   */
  private String convertPathValueForArrays(String pathSegment, boolean isArrayValue) {
    String pathValue;
    if (isArrayValue) {
      int idx = Integer.parseInt(pathSegment);
      if (idx > config.getMaxArrayLength() - 1) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
      }
      // left-pad the array element to 6 characters
      pathValue = "[" + DocsApiUtils.leftPadTo6(pathSegment) + "]";
    } else {
      pathValue = pathSegment;
    }

    return pathValue;
  }

  /**
   * Takes a payload that is a URL-encoded form and turns it into data (bound parameter lists) to
   * write to a collection. Also returns a list of all "top-level" keys in the form, for the
   * purposes of merging key sets in the event of a PATCH operation. This is only used by certain
   * performance tests, is not recommended for use, and will be deleted in the near future.
   *
   * @param path
   * @param key
   * @param formPayload
   * @param patching
   * @param numericBooleans
   * @return
   */
  @Deprecated
  private ImmutablePair<List<Object[]>, List<String>> shredForm(
      List<String> path,
      String key,
      String formPayload,
      boolean patching,
      boolean numericBooleans) {
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

      if (path.size() + fieldNames.length > config.getMaxDepth()) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
      }

      Map<String, Object> bindMap = DocsApiUtils.newBindMap(path, config.getMaxDepth());
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
          if (idx > config.getMaxArrayLength() - 1) {
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
      bindMap.put(DocsApiConstants.LEAF_COLUMN_NAME, leaf);

      if (value.equals("null")) {
        bindMap.put("dbl_value", null);
        bindMap.put("bool_value", null);
        bindMap.put("text_value", null);
      } else if (value.equals("true") || value.equals("false")) {
        bindMap.put(
            DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME,
            convertToBackendBooleanValue(Boolean.parseBoolean(value), numericBooleans));
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
          bindMap.put(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME, doubleValue);
        } else {
          bindMap.put(DocsApiConstants.STRING_VALUE_COLUMN_NAME, value);
        }
      }
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
