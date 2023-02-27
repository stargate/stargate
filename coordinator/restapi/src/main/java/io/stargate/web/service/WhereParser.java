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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltCondition.LHS;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.web.resources.Converters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WhereParser {
  private static final ObjectMapper mapper = new ObjectMapper();

  // Throws an exception on duplicate keys like
  //    {"key1": 1, "key1": 2} or {"key1": {"eq": 1, "eq":2}}
  static {
    mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  }

  public static List<BuiltCondition> parseWhere(String whereParam, AbstractTable tableData)
      throws IOException {
    JsonNode jsonTree = parseJson(whereParam);

    final List<BuiltCondition> conditions = new ArrayList<>();

    for (Map.Entry<String, JsonNode> whereCondition : asIterable(jsonTree.fields())) {
      String fieldName = whereCondition.getKey();
      Column column = getColumn(tableData, fieldName);

      JsonNode condition = whereCondition.getValue();
      if (!condition.isObject()) {
        throw new IllegalArgumentException(
            String.format("Entry for field %s was expecting a JSON object as input.", fieldName));
      }

      for (Map.Entry<String, JsonNode> operandAndValue : asIterable(condition.fields())) {
        String rawOp = operandAndValue.getKey();
        FilterOp operator = decode(rawOp);

        JsonNode value = operandAndValue.getValue();
        if (value.isNull()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value entry for field %s, operation %s was expecting a value, but found null.",
                  fieldName, rawOp));
        }

        QueryContext context = new QueryContext();
        context.fieldName = fieldName;
        context.rawOp = rawOp;
        context.operator = operator;
        context.value = value;
        context.type = column.type();

        switch (operator) {
          case $IN:
            evaluateIn(conditions, context);
            break;
          case $EXISTS:
            evaluateExists(conditions, context);
            break;
          case $CONTAINS:
            evaluateContains(conditions, context);
            break;
          case $CONTAINSKEY:
            evaluateContainsKey(conditions, context);
            break;
          case $CONTAINSENTRY:
            evaluateContainsEntry(conditions, context);
            break;
          default: // GT, GTE, LT, LTE, EQ, NE
            {
              addToCondition(conditions, context);
              break;
            }
        }
      }
    }

    return Collections.unmodifiableList(conditions);
  }

  private static FilterOp decode(String rawOp) {
    try {
      return FilterOp.valueOf(rawOp.toUpperCase());
    } catch (IllegalArgumentException iea) {
      throw new IllegalArgumentException(String.format("Operation %s is not supported.", rawOp));
    }
  }

  private static JsonNode parseJson(String whereParam) {
    try {
      JsonNode input = mapper.readTree(whereParam);
      if (!input.isObject()) {
        throw new IllegalArgumentException(
            "Was expecting a JSON object as input for where parameter.");
      }
      return input;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Input provided is not valid json. %s", e.getMessage()));
    }
  }

  private static <T> Iterable<T> asIterable(Iterator<T> iterator) {
    return () -> iterator;
  }

  private static Column getColumn(AbstractTable tableData, String fieldName) {
    Column column = tableData.column(fieldName);
    if (column == null) {
      throw new IllegalArgumentException(String.format("Unknown field name '%s'.", fieldName));
    }
    return column;
  }

  private static void addToCondition(List<BuiltCondition> conditions, QueryContext context) {
    if (context.value.isArray()) {
      for (JsonNode element : asIterable(context.value.elements())) {
        Object val = Converters.toCqlValue(context.type, element.asText());
        conditions.add(conditionToWhere(context.fieldName, context.operator, val));
      }
    } else {
      Object val = Converters.toCqlValue(context.type, context.value.asText());
      conditions.add(conditionToWhere(context.fieldName, context.operator, val));
    }
  }

  private static BuiltCondition conditionToWhere(String fieldName, FilterOp op, Object value) {
    return BuiltCondition.of(fieldName, op.predicate, value);
  }

  private static void evaluateContainsKey(List<BuiltCondition> conditions, QueryContext context) {
    if (context.type == null || !context.type.isMap()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for map types",
              context.fieldName, context.rawOp));
    }
    context.type = context.type.parameters().get(0);
    addToCondition(conditions, context);
  }

  private static void evaluateContains(List<BuiltCondition> conditions, QueryContext context) {
    if (context.type == null || !context.type.isCollection()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for collection types",
              context.fieldName, context.rawOp));
    }
    context.type =
        context.type.isMap() ? context.type.parameters().get(1) : context.type.parameters().get(0);
    addToCondition(conditions, context);
  }

  private static void evaluateExists(List<BuiltCondition> conditions, QueryContext context) {
    if (!context.value.isBoolean() || !context.value.booleanValue()) {
      throw new IllegalArgumentException("`exists` only supports the value `true`.");
    }
    conditions.add(conditionToWhere(context.fieldName, context.operator, true));
  }

  private static void evaluateContainsEntry(List<BuiltCondition> conditions, QueryContext context) {
    if (context.value.isObject()) {
      addEntryCondition(conditions, context);
    } else if (context.value.isArray()) {
      JsonNode entries = context.value;
      for (JsonNode entry : asIterable(entries.elements())) {
        context.value = entry;
        addEntryCondition(conditions, context);
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an object "
                  + "with two fields 'key' and 'value' or an array of those objects.",
              context.fieldName, context.rawOp));
    }
  }

  private static void addEntryCondition(List<BuiltCondition> conditions, QueryContext context) {
    JsonNode entryKey, entryValue;
    if (!context.value.isObject()
        || context.value.size() != 2
        || (entryKey = context.value.get("key")) == null
        || (entryValue = context.value.get("value")) == null) {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an object "
                  + "with two fields 'key' and 'value'.",
              context.fieldName, context.rawOp));
    }

    if (context.type == null || !context.type.isMap()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for map types",
              context.fieldName, context.rawOp));
    }
    Column.ColumnType keyType = context.type.parameters().get(0);
    Column.ColumnType valueType = context.type.parameters().get(1);
    Object mapKey = Converters.toCqlValue(keyType, entryKey.asText());
    Object mapValue = Converters.toCqlValue(valueType, entryValue.asText());
    conditions.add(
        BuiltCondition.of(
            LHS.mapAccess(context.fieldName, mapKey), context.operator.predicate, mapValue));
  }

  private static void evaluateIn(List<BuiltCondition> conditions, QueryContext context)
      throws IOException {
    if (!context.value.isArray()) {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an array.",
              context.fieldName, context.rawOp));
    }
    ObjectReader reader = mapper.readerFor(new TypeReference<List<Object>>() {});

    List<Object> rawList = reader.readValue(context.value);
    conditions.add(
        conditionToWhere(
            context.fieldName,
            context.operator,
            rawList.stream()
                .map(obj -> Converters.toCqlValue(context.type, obj))
                .collect(Collectors.toList())));
  }

  private static class QueryContext {
    public String fieldName;
    public String rawOp;
    public FilterOp operator;
    public JsonNode value;
    public Column.ColumnType type;
  }
}
