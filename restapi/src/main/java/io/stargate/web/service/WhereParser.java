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
import io.stargate.db.datastore.query.ImmutableWhereCondition;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Table;
import io.stargate.web.resources.Converters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WhereParser {
  private static final ObjectMapper mapper = new ObjectMapper();

  public static List<Where<?>> parseWhere(String whereParam, Table tableData) throws IOException {
    JsonNode filterJson;
    try {
      filterJson = mapper.readTree(whereParam);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Input provided is not valid json");
    }
    List<Where<?>> conditions = new ArrayList<>();

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
        String op = ops.next();
        JsonNode value = fieldConditions.get(op);
        if (!value.isNull() && value.isArray() && op.equalsIgnoreCase(FilterOp.$IN.rawValue)) {

          ObjectReader reader = mapper.readerFor(new TypeReference<List<Object>>() {});
          conditions.add(conditionToWhere(fieldName, op, reader.readValue(value)));
          continue;
        }

        if (!value.isValueNode() || value.isNull()) {
          throw new RuntimeException(
              String.format(
                  "Value entry for field %s, operation %s was expecting a value, but found an object, array, or null.",
                  fieldName, op));
        }

        try {
          FilterOp.valueOf(op.toUpperCase());
        } catch (IllegalArgumentException iea) {
          throw new RuntimeException(String.format("Operation %s is not supported", op));
        }

        if (op.equalsIgnoreCase(FilterOp.$EXISTS.rawValue)) {
          if (!value.isBoolean() || !value.booleanValue()) {
            throw new RuntimeException("`exists` only supports the value `true`");
          }
          conditions.add(conditionToWhere(fieldName, op, true));
        } else {
          Column.ColumnType type = tableData.column(fieldName).type();
          Object val = value.asText();

          if (type != null) {
            val = Converters.typeForValue(type, value.asText());
          }

          conditions.add(conditionToWhere(fieldName, op, val));
        }
      }
    }

    return conditions;
  }

  private static Where<?> conditionToWhere(String fieldName, String op, Object value) {
    return ImmutableWhereCondition.builder()
        .value(value)
        .predicate(FilterOp.valueOf(op.toUpperCase()).predicate)
        .column(fieldName.toLowerCase())
        .build();
  }
}
