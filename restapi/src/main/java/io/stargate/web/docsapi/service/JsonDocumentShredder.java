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

  private final DocsApiConfiguration config;
  private final ObjectMapper objectMapper;

  @Inject
  public JsonDocumentShredder(DocsApiConfiguration config, ObjectMapper objectMapper) {
    this.config = config;
    this.objectMapper = objectMapper;
  }

  /**
   * Shreds the JSON payload and returns the list of {@link JsonShreddedRow} for each value that
   * should be stored in the data store.
   *
   * @param payload JSON payload as string
   * @param subDocumentPath Prefix path to use. Note that paths are added to each row path as they
   *     are given, without any modifications.
   * @return List of shredded rows
   */
  public List<JsonShreddedRow> shred(String payload, List<String> subDocumentPath) {
    try {
      JsonNode node = objectMapper.readTree(payload);
      return shred(node, subDocumentPath);
    } catch (JsonProcessingException e) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE,
          "Malformed JSON object found during shredding.",
          e);
    }
  }

  /**
   * Shreds the {@link JsonNode} and returns the list of {@link JsonShreddedRow} for each value that
   * should be stored in the data store.
   *
   * @param node {@link JsonNode}
   * @param subDocumentPath Prefix path to use. Note that paths are added to each row path as they
   *     are given, without any modifications.
   * @return List of shredded rows
   */
  public List<JsonShreddedRow> shred(JsonNode node, List<String> subDocumentPath) {
    // check if this is a valid root node
    if (subDocumentPath.isEmpty()) {
      checkRoot(node);
    }

    Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder =
        () ->
            ImmutableJsonShreddedRow.builder()
                .maxDepth(config.getMaxDepth())
                .addAllPath(subDocumentPath);

    List<JsonShreddedRow> result = new ArrayList<>();
    processNode(node, rowBuilder, result);
    return result;
  }

  private void checkRoot(JsonNode root) {
    // empty object and arrays not allowed
    if (root.isContainerNode() && root.isEmpty()) {
      String msg =
          "Updating a key with just an empty object or an empty array is not allowed. Hint: update the parent path with a defined object instead.";
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PUT_PAYLOAD_INVALID, msg);
    }

    // scalars not allowed
    if (root.isValueNode()) {
      String msg =
          "Updating a key with just a JSON primitive is not allowed. Hint: update the parent path with a defined object instead.";
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_PUT_PAYLOAD_INVALID, msg);
    }
  }

  private void processNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      List<JsonShreddedRow> result) {
    if (node.isArray()) {
      processArrayNode(node, rowBuilder, result);
    } else if (node.isObject()) {
      processObjectNode(node, rowBuilder, result);
    } else {
      processValueNode(node, rowBuilder, result);
    }
  }

  private void processArrayNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      List<JsonShreddedRow> result) {
    // empty array, simply create a reference to empty node and return
    if (node.isEmpty()) {
      ImmutableJsonShreddedRow row =
          rowBuilder.get().stringValue(DocsApiConstants.EMPTY_ARRAY_MARKER).build();
      result.add(row);
      return;
    }

    // make sure we are not overflowing the array
    if (node.size() > config.getMaxArrayLength()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
    }

    // otherwise, iterate all nodes
    int idx = 0;
    for (JsonNode inner : node) {
      // convert the array index into path
      // then create new next row builder
      String arrayPath = "[" + DocsApiUtils.leftPadTo6(String.valueOf(idx)) + "]";
      Supplier<ImmutableJsonShreddedRow.Builder> nextRowBuilder =
          () -> rowBuilder.get().addPath(arrayPath);

      // process inner node and increase the index
      processNode(inner, nextRowBuilder, result);
      idx++;
    }
  }

  private void processObjectNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
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
              String fieldName = field.getKey();

              if (fieldName.isEmpty()) {
                String msg =
                    "JSON objects containing empty field names are not supported at the moment.";
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME, msg);
              }

              // check for valid field name
              if (DocsApiUtils.containsIllegalSequences(fieldName)) {
                String msg =
                    String.format(
                        "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field %s",
                        fieldName);
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME, msg);
              }

              // escape the field path
              // then create new next row builder
              String fieldPath = DocsApiUtils.convertEscapedCharacters(fieldName);
              Supplier<ImmutableJsonShreddedRow.Builder> nextRowBuilder =
                  () -> rowBuilder.get().addPath(fieldPath);

              // process inner node and increase the index
              processNode(field.getValue(), nextRowBuilder, result);
            });
  }

  private void processValueNode(
      JsonNode node,
      Supplier<ImmutableJsonShreddedRow.Builder> rowBuilder,
      List<JsonShreddedRow> result) {
    ImmutableJsonShreddedRow.Builder builder = rowBuilder.get();

    // depending on the value type set values
    if (node.isBoolean()) {
      builder.booleanValue(node.asBoolean());
    } else if (node.isNumber()) {
      builder.doubleValue(node.asDouble());
    } else if (!node.isNull()) {
      builder.stringValue(node.asText());
    }

    // build and add to the results
    ImmutableJsonShreddedRow row = builder.build();
    result.add(row);
  }
}
