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

package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;

public class JsonDocumentShredder {

  @Inject private DocsApiConfiguration config;

  @Inject private ObjectMapper objectMapper;

  public JsonDocumentShredder(DocsApiConfiguration config, ObjectMapper objectMapper) {
    this.config = config;
    this.objectMapper = objectMapper;
  }

  public List<JsonShreddedRow> shred(
      String payload, String documentId, List<String> subDocumentPath, boolean numericBooleans) {
    try {
      JsonNode node = objectMapper.readTree(payload);
      return shred(node, documentId, subDocumentPath, numericBooleans);
    } catch (JsonProcessingException e) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE,
          "Malformed JSON object found during shredding.",
          e);
    }
  }

  public List<JsonShreddedRow> shred(
      JsonNode node, String documentId, List<String> subDocumentPath, boolean numericBooleans) {
    Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder =
        () ->
            ImmutableJsonShreddedRow.builder()
                .key(documentId)
                .maxDepth(config.getMaxDepth())
                .addAllPath(subDocumentPath); // TODO should we also convert these parts with escape

    List<JsonShreddedRow> result = new ArrayList<>();
    processNode(node, rowBuilder, numericBooleans, result);
    return result;
  }

  private void processNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      boolean numericBooleans,
      List<JsonShreddedRow> result) {
    if (node.isArray()) {
      processArrayNode(node, rowBuilder, numericBooleans, result);
    } else if (node.isObject()) {
      processObjectNode(node, rowBuilder, numericBooleans, result);
    } else {
      processValueNode(node, rowBuilder, numericBooleans, result);
    }
  }

  private void processArrayNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      boolean numericBooleans,
      List<JsonShreddedRow> result) {
    // empty array, simply create a reference to empty node and return
    if (node.isEmpty()) {
      ImmutableJsonShreddedRow row =
          rowBuilder.get().stringValue(DocsApiConstants.EMPTY_ARRAY_MARKER).build();
      result.add(row);
      return;
    }

    // otherwise, iterate all nodes
    int idx = 0;
    for (JsonNode inner : node) {
      // make sure we didn't exceed the maximum array length
      if (idx >= config.getMaxArrayLength()) {
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
      }

      // convert the array index into path
      // then create new next row builder
      String arrayPath = "[" + DocsApiUtils.leftPadTo6(String.valueOf(idx)) + "]";
      Supplier<ImmutableJsonShreddedRow.Builder> nextRowBuilder =
          () -> rowBuilder.get().addPath(arrayPath);

      // process inner node and increase the index
      processNode(inner, nextRowBuilder, numericBooleans, result);
      idx++;
    }
  }

  private void processObjectNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      boolean numericBooleans,
      List<JsonShreddedRow> result) {
    // empty object, simply create a reference to empty node and return
    if (node.isEmpty()) {
      ImmutableJsonShreddedRow row =
          rowBuilder.get().stringValue(DocsApiConstants.EMPTY_OBJECT_MARKER).build();
      result.add(row);
      return;
    }

    node.fields()
        .forEachRemaining(
            field -> {
              // TODO is this still valid
              //  ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME

              // escape the field path
              // then create new next row builder
              String fieldPath = DocsApiUtils.convertEscapedCharacters(field.getKey());
              Supplier<ImmutableJsonShreddedRow.Builder> nextRowBuilder =
                  () -> rowBuilder.get().addPath(fieldPath);

              // process inner node and increase the index
              processNode(field.getValue(), nextRowBuilder, numericBooleans, result);
            });
  }

  private void processValueNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      boolean numericBooleans,
      List<JsonShreddedRow> result) {
    ImmutableJsonShreddedRow.Builder builder = rowBuilder.get();

    // depending on the value type set values
    if (node.isBoolean()) {
      builder.booleanValue(convertToBackendBooleanValue(node.asBoolean(), numericBooleans));
    } else if (node.isNumber()) {
      builder.doubleValue(node.asDouble());
    } else if (!node.isNull()) {
      builder.stringValue(node.asText());
    }

    // build and add to the results
    ImmutableJsonShreddedRow row = builder.build();
    result.add(row);
  }

  private Object convertToBackendBooleanValue(boolean value, boolean numericBooleans) {
    if (numericBooleans) {
      return value ? 1 : 0;
    }
    return value;
  }
}
