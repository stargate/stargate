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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltCondition.LHS;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.web.resources.Converters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WhereParser {
  private static final ObjectMapper mapper = new ObjectMapper();

  public static List<BuiltCondition> parseWhere(String whereParam, Table tableData)
      throws IOException {
    JsonNode filterJson;
    try {
      filterJson = mapper.readTree(whereParam);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Input provided is not valid json");
    }
    List<BuiltCondition> conditions = new ArrayList<>();

    if (!filterJson.isObject()) {
      throw new RuntimeException("Was expecting a JSON object as input for where parameter.");
    }

    ObjectNode input = (ObjectNode) filterJson;
    Iterator<String> fields = input.fieldNames();
    while (fields.hasNext()) {
      String fieldName = fields.next();
      JsonNode fieldConditions = input.get(fieldName);

      if (!fieldConditions.isObject()) {
        throw new RuntimeException(
            String.format("Entry for field %s was expecting a JSON object as input.", fieldName));
      }

      Iterator<String> ops = fieldConditions.fieldNames();
      while (ops.hasNext()) {
        String rawOp = ops.next();
        FilterOp op;
        try {
          op = FilterOp.valueOf(rawOp.toUpperCase());
        } catch (IllegalArgumentException iea) {
          throw new RuntimeException(String.format("Operation %s is not supported", rawOp));
        }

        JsonNode value = fieldConditions.get(rawOp);
        if (value.isNull()) {
          throw new RuntimeException(
              String.format(
                  "Value entry for field %s, operation %s was expecting a value, but found null.",
                  fieldName, rawOp));
        }

        if (op == FilterOp.$IN) {
          if (!value.isArray()) {
            throw new RuntimeException(
                String.format(
                    "Value entry for field %s, operation %s must be an array.", fieldName, rawOp));
          }
          ObjectReader reader = mapper.readerFor(new TypeReference<List<Object>>() {});
          conditions.add(conditionToWhere(fieldName, op, reader.readValue(value)));
        } else if (op == FilterOp.$CONTAINSENTRY) {
          JsonNode entryKey, entryValue;
          if (!value.isObject()
              || value.size() != 2
              || (entryKey = value.get("key")) == null
              || (entryValue = value.get("value")) == null) {
            throw new RuntimeException(
                String.format(
                    "Value entry for field %s, operation %s must be an object "
                        + "with two fields 'key' and 'value'.",
                    fieldName, rawOp));
          }
          Column.ColumnType mapType = tableData.column(fieldName).type();
          if (mapType == null || !mapType.isMap()) {
            throw new RuntimeException(
                String.format(
                    "Field %s: operation %s is only supported for map types", fieldName, rawOp));
          }
          Column.ColumnType keyType = mapType.parameters().get(0);
          Column.ColumnType valueType = mapType.parameters().get(1);
          Object mapKey = Converters.typeForValue(keyType, entryKey.asText());
          Object mapValue = Converters.typeForValue(valueType, entryValue.asText());
          conditions.add(
              BuiltCondition.of(
                  LHS.mapAccess(fieldName.toLowerCase(), mapKey), op.predicate, mapValue));
        } else {
          // Remaining operators: the value is a simple node
          if (!value.isValueNode()) {
            throw new RuntimeException(
                String.format(
                    "Value entry for field %s, operation %s was expecting a value, but found an object or array.",
                    fieldName, rawOp));
          }

          if (op == FilterOp.$EXISTS) {
            if (!value.isBoolean() || !value.booleanValue()) {
              throw new RuntimeException("`exists` only supports the value `true`");
            }
            conditions.add(conditionToWhere(fieldName, op, true));
          } else {
            Object val = value.asText();
            Column.ColumnType columnType = tableData.column(fieldName).type();
            if (columnType != null) {
              Column.ColumnType valueType;
              if (op == FilterOp.$CONTAINS) {
                if (columnType.isCollection()) {
                  valueType =
                      columnType.isMap()
                          ? columnType.parameters().get(1)
                          : columnType.parameters().get(0);
                } else {
                  throw new RuntimeException(
                      String.format(
                          "Field %s: operation %s is only supported for collection types",
                          fieldName, rawOp));
                }
              } else if (op == FilterOp.$CONTAINSKEY) {
                if (columnType.isMap()) {
                  valueType = columnType.parameters().get(0);
                } else {
                  throw new RuntimeException(
                      String.format(
                          "Field %s: operation %s is only supported for map types",
                          fieldName, rawOp));
                }
              } else {
                valueType = columnType;
              }
              val = Converters.typeForValue(valueType, value.asText());
            }
            conditions.add(conditionToWhere(fieldName, op, val));
          }
        }
      }
    }

    return conditions;
  }

  private static BuiltCondition conditionToWhere(String fieldName, FilterOp op, Object value) {
    return BuiltCondition.of(fieldName.toLowerCase(), op.predicate, value);
  }
}
