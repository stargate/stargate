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

package io.stargate.web.docsapi.service.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.base.Splitter;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.exception.RuntimeExceptionPassHandlingStrategy;
import io.stargate.web.docsapi.service.query.QueryConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.path.JsonPath;
import org.jsfr.json.path.PathOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DocsApiUtils {
  private static final Logger logger = LoggerFactory.getLogger(DocsApiUtils.class);
  public static final Pattern PERIOD_PATTERN = Pattern.compile("(?<!\\\\)\\.");
  public static final Pattern COMMA_PATTERN = Pattern.compile("(?<!\\\\),");
  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[.*\\]");
  public static final Pattern ESCAPED_PATTERN = Pattern.compile("(\\\\,|\\\\\\.|\\\\\\*)");
  public static final Pattern ESCAPED_PATTERN_INTERNAL_CAPTURE = Pattern.compile("\\\\(\\.|\\*|,)");
  private static final Splitter FORM_SPLITTER = Splitter.on('&');
  private static final Splitter PAIR_SPLITTER = Splitter.on('=');

  private DocsApiUtils() {}

  /**
   * Converts given path to an array one if needed:
   *
   * <ol>
   *   <li>[1] - converted to [000001]
   *   <li>[0],[1] - converted to [000000],[000001]
   *   <li>[*] - not converted
   *   <li>anyPath - not converted
   * </ol>
   *
   * @param path single filter or field path
   * @return Converted to represent expected path in DB, keeping the segmentation.
   */
  public static String convertArrayPath(String path) {
    if (path.contains(",")) {
      return Arrays.stream(path.split(COMMA_PATTERN.pattern()))
          .map(DocsApiUtils::convertSingleArrayPath)
          .collect(Collectors.joining(","));
    } else {
      return convertSingleArrayPath(path);
    }
  }

  /**
   * Converts any of the valid escape sequences (for periods, commas, and asterisks) into their
   * actual character. E.g. if the input string is literally abc\.123, this function returns abc.123
   * This allows a user to use escape sequences in where filters and when writing documents when it
   * would otherwise be ambiguous to use the corresponding character.
   *
   * @param path single filter or field path
   * @return Converted to a string with no literal unicode code points.
   */
  public static String convertEscapedCharacters(String path) {
    return path.replaceAll(ESCAPED_PATTERN_INTERNAL_CAPTURE.pattern(), "$1");
  }

  public static List<String> convertEscapedCharacters(List<String> path) {
    return path.stream().map(DocsApiUtils::convertEscapedCharacters).collect(Collectors.toList());
  }

  private static String convertSingleArrayPath(String path) {
    return extractArrayPathIndex(path)
        .map(innerPath -> "[" + leftPadTo6(innerPath.toString()) + "]")
        .orElse(path);
  }

  /**
   * Optionally extracts the index of the array path. Returns empty if the path is not an array
   * path.
   *
   * @param path single filter or field path
   * @return Array index or empty
   */
  public static Optional<Integer> extractArrayPathIndex(String path) {
    // check if we have array path
    if (ARRAY_PATH_PATTERN.matcher(path).matches()) {
      String innerPath = path.substring(1, path.length() - 1);
      // if it's wildcard keep as it is
      if (!Objects.equals(innerPath, DocumentDB.GLOB_VALUE)) {
        // otherwise try to parse int
        try {
          // this can fail, thus wrap in the try
          int idx = Integer.parseInt(innerPath);
          if (idx > DocumentDB.MAX_ARRAY_LENGTH - 1) {
            String msg =
                String.format("Max array length of %s exceeded.", DocumentDB.MAX_ARRAY_LENGTH);
            throw new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED, msg);
          }
          return Optional.of(idx);
        } catch (NumberFormatException e) {
          String msg = String.format("Array path %s is not valid.", path);
          throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID, msg);
        }
      }
    }
    return Optional.empty();
  }

  private static Object convertToBackendBooleanValue(boolean value, boolean numericBooleans) {
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
  public static ImmutablePair<List<Object[]>, List<String>> shredPayload(
      JsonSurfer surfer,
      DocumentDB db,
      List<String> path,
      String key,
      String payload,
      int maxDepth,
      int maxArrayLength,
      boolean patching,
      boolean isJson) {
    String trimmed = payload.trim();
    if (isJson) {
      return shredJson(surfer, db, path, key, trimmed, maxDepth, maxArrayLength, patching);
    } else {
      return shredForm(db, path, key, trimmed, maxDepth, maxArrayLength, patching);
    }
  }

  public static ImmutablePair<List<Object[]>, List<String>> shredJson(
      JsonSurfer surfer,
      DocumentDB db,
      List<String> path,
      String key,
      String jsonPayload,
      int maxDepth,
      int maxArrayLength,
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
                    if (i >= maxDepth) {
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
                      if (idx > maxArrayLength - 1) {
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

  private static ImmutablePair<List<Object[]>, List<String>> shredForm(
      DocumentDB db,
      List<String> path,
      String key,
      String formPayload,
      int maxDepth,
      int maxArrayLength,
      boolean patching) {
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

      if (path.size() + fieldNames.length > maxDepth) {
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
          if (idx > maxArrayLength - 1) {
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

  private static boolean isEmptyObject(JsonNode v) {
    return v.isObject() && v.isEmpty();
  }

  private static boolean isEmptyArray(JsonNode v) {
    return v.isArray() && v.isEmpty();
  }

  public static String leftPadTo6(String value) {
    return StringUtils.leftPad(value, 6, '0');
  }

  /**
   * Returns the collection of paths that are represented by each JsonNode in the #fieldsJson.
   *
   * <p>For each field a list of strings representing the path is returned. Note that for each path
   * member, {@link #convertArrayPath(String)} will be called.
   *
   * @param fieldsJson array json node
   * @return collection of paths representing all fields
   */
  public static Collection<List<String>> convertFieldsToPaths(JsonNode fieldsJson) {
    if (!fieldsJson.isArray()) {
      String msg = String.format("`fields` must be a JSON array, found %s", fieldsJson);
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, msg);
    }

    Collection<List<String>> results = new ArrayList<>();
    for (JsonNode value : fieldsJson) {
      if (!value.isTextual()) {
        String msg = String.format("Each field must be a string, found %s", value);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, msg);
      }
      String fieldValue = value.asText();
      List<String> fieldPath =
          Arrays.stream(fieldValue.split(PERIOD_PATTERN.pattern()))
              .map(DocsApiUtils::convertArrayPath)
              .map(DocsApiUtils::convertEscapedCharacters)
              .collect(Collectors.toList());

      results.add(fieldPath);
    }

    return results;
  }

  public static boolean containsIllegalSequences(String x) {
    String replaced = x.replaceAll(DocsApiUtils.ESCAPED_PATTERN.pattern(), "");
    return replaced.contains("[")
        || replaced.contains(".")
        || replaced.contains("'")
        || replaced.contains("\\");
  }

  /**
   * Converts a JSON path string (e.g. "$.a.b.c[0]") into a JSON path string that only uses square
   * brackets to denote pathing (e.g. "$['a']['b']['c'][0]". This is to allow escaping of certain
   * characters, such as space, $, and @.
   */
  public static String convertJsonToBracketedPath(String path) {
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

  /**
   * Simple utility to return a string value from doc api {@link Row}.
   *
   * @param row Row
   * @return String or null if a row column {@value QueryConstants#STRING_VALUE_COLUMN_NAME) is null.
   */
  public static String getStringFromRow(Row row) {
    return row.isNull(QueryConstants.STRING_VALUE_COLUMN_NAME)
        ? null
        : row.getString(QueryConstants.STRING_VALUE_COLUMN_NAME);
  }

  /**
   * Simple utility to return a double value from doc api {@link Row}.
   *
   * @param row Row
   * @return String or null if a row column {@value QueryConstants#DOUBLE_VALUE_COLUMN_NAME) is null.
   */
  public static Double getDoubleFromRow(Row row) {
    return row.isNull(QueryConstants.DOUBLE_VALUE_COLUMN_NAME)
        ? null
        : row.getDouble(QueryConstants.DOUBLE_VALUE_COLUMN_NAME);
  }

  /**
   * Simple utility to return boolean value from doc api {@link Row} based on number booleans value.
   *
   * @param row Row
   * @param numericBooleans Consider booleans as numeric
   * @return True, False or null if a row column {@value QueryConstants#BOOLEAN_VALUE_COLUMN_NAME) is null.
   */
  public static Boolean getBooleanFromRow(Row row, boolean numericBooleans) {
    boolean nullValue = row.isNull(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME);
    if (nullValue) {
      return null;
    } else {
      if (numericBooleans) {
        byte value = row.getByte(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME);
        return value != 0;
      } else {
        return row.getBoolean(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME);
      }
    }
  }

  /**
   * Tests if the given row exactly matches the path, where path is defined by the list of strings.
   *
   * <p>This checks:
   *
   * <ol>
   *   <li>ignores globs in #pathIterable
   *   <li>handles correctly path segments in #pathIterable (f.e. ['first,second', 'value']) will
   *       match row on `first.value` or `second.value`
   *   <li>
   *   <li>proves that path is not sub-path of row's path
   * </ol>
   *
   * @param row Row
   * @param path path as iterable strings
   * @return True if row is matching on the given path
   */
  public static boolean isRowMatchingPath(Row row, List<String> path) {
    int targetPathSize = path.size();

    // short-circuit if the field is not matching
    // we expect leaf to be always fetched
    String field = path.get(targetPathSize - 1);
    String leaf = row.getString(QueryConstants.LEAF_COLUMN_NAME);
    if (!Objects.equals(DocsApiUtils.convertEscapedCharacters(field), leaf)) {
      return false;
    }

    // short-circuit if p_n after path is not empty
    String targetP = QueryConstants.P_COLUMN_NAME.apply(targetPathSize);
    boolean exists = row.columnExists(targetP);
    if (!exists || !Objects.equals(row.getString(targetP), "")) {
      return false;
    }

    // then as last resort confirm the path is matching
    return DocsApiUtils.isRowOnPath(row, path);
  }

  /**
   * Tests if the given row is on the path, where path is defined by the iterable of strings.
   *
   * <p>This checks:
   *
   * <ol>
   *   <li>ignores globs in #pathIterable
   *   <li>handles correctly path segments in #pathIterable (f.e. ['first,second', 'value']) will
   *       match row on `first.value` or `second.value`
   *   <li>
   * </ol>
   *
   * @param row Row
   * @param pathIterable path as iterable strings
   * @return True if row is fully on the given path
   */
  public static boolean isRowOnPath(Row row, Iterable<String> pathIterable) {
    int p = 0;
    for (String target : pathIterable) {
      int index = p++;
      // check that row has the request path depth
      String targetP = QueryConstants.P_COLUMN_NAME.apply(index);
      boolean exists = row.columnExists(targetP);
      if (!exists) {
        return false;
      }

      // get the path
      String path = row.getString(targetP);

      // skip any target path that is a wildcard
      if (Objects.equals(target, DocumentDB.GLOB_VALUE)) {
        continue;
      }

      // skip any target path that is an array wildcard
      if (Objects.equals(target, DocumentDB.GLOB_ARRAY_VALUE)) {
        // but make sure this is not an normal field
        if (!ARRAY_PATH_PATTERN.matcher(path).matches()) {
          return false;
        }
        continue;
      }

      boolean noneMatch =
          Arrays.stream(target.split(COMMA_PATTERN.pattern()))
              .noneMatch(t -> Objects.equals(DocsApiUtils.convertEscapedCharacters(t), path));
      if (noneMatch) {
        return false;
      }
    }

    return true;
  }
}
