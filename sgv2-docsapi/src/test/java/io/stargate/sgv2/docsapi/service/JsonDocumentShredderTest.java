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

package io.stargate.sgv2.docsapi.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(JsonDocumentShredderTest.Profile.class)
class JsonDocumentShredderTest {

  @Inject JsonDocumentShredder shredder;

  private ObjectMapper objectMapper;

  @Inject DocumentProperties configuration;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.document.max-depth", "3")
          .put("stargate.document.max-array-length", "2")
          .build();
    }
  }

  @BeforeEach
  public void init() {
    objectMapper = new ObjectMapper();
    shredder = new JsonDocumentShredder(configuration, objectMapper);
  }

  @Nested
  class Shred {

    @Test
    public void primitive() throws JsonProcessingException {
      JsonNode payload = objectMapper.readTree("22");

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.singletonList("field"));

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isNull();
                assertThat(row.getDoubleValue()).isEqualTo(22d);
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void primitiveRoot() throws JsonProcessingException {
      JsonNode payload = objectMapper.readTree("22");

      Throwable result = catchThrowable(() -> shredder.shred(payload, Collections.emptyList()));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_PUT_PAYLOAD_INVALID);
    }

    @Test
    public void simpleObjectStringValue() {
      ObjectNode payload = objectMapper.createObjectNode().put("field", "text");

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isEqualTo("text");
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void simpleObjectNumericValue() {
      ObjectNode payload = objectMapper.createObjectNode().put("field", 22);

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isNull();
                assertThat(row.getDoubleValue()).isEqualTo(22d);
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void simpleObjectBooleanValue() {
      ObjectNode payload = objectMapper.createObjectNode().put("field", true);

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isNull();
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isEqualTo(Boolean.TRUE);
              });
    }

    @Test
    public void simpleObjectNullValue() {
      ObjectNode payload = objectMapper.createObjectNode().putNull("field");

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isNull();
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void emptyFieldName() {
      ObjectNode payload = objectMapper.createObjectNode().put("", "text");

      Throwable result = catchThrowable(() -> shredder.shred(payload, Collections.emptyList()));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME);
    }

    @Test
    public void emptyObject() {
      ObjectNode payload = objectMapper.createObjectNode();

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.singletonList("path"));

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("path");
                assertThat(row.getLeaf()).isEqualTo("path");
                assertThat(row.getStringValue()).isEqualTo(Constants.EMPTY_OBJECT_MARKER);
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void emptyObjectRoot() {
      ObjectNode payload = objectMapper.createObjectNode();

      Throwable result = catchThrowable(() -> shredder.shred(payload, Collections.emptyList()));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_PUT_PAYLOAD_INVALID);
    }

    @Test
    public void withEmptyObject() {
      ObjectNode payload =
          objectMapper.createObjectNode().set("field", objectMapper.createObjectNode());

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isEqualTo(Constants.EMPTY_OBJECT_MARKER);
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void withNestedObject() {
      ObjectNode n1 = objectMapper.createObjectNode().put("key1", "value1");
      ObjectNode n2 = objectMapper.createObjectNode().put("key2", 44.22d);
      ObjectNode payload = objectMapper.createObjectNode();
      payload.set("n1", n1);
      payload.set("n2", n2);

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .hasSize(2)
          .anySatisfy(
              row -> {
                assertThat(row.getPath()).containsExactly("n1", "key1");
                assertThat(row.getLeaf()).isEqualTo("key1");
                assertThat(row.getStringValue()).isEqualTo("value1");
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              })
          .anySatisfy(
              row -> {
                assertThat(row.getPath()).containsExactly("n2", "key2");
                assertThat(row.getLeaf()).isEqualTo("key2");
                assertThat(row.getStringValue()).isNull();
                assertThat(row.getDoubleValue()).isEqualTo(44.22d);
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void emptyArray() {
      ArrayNode payload = objectMapper.createArrayNode();

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.singletonList("path"));

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("path");
                assertThat(row.getLeaf()).isEqualTo("path");
                assertThat(row.getStringValue()).isEqualTo(Constants.EMPTY_ARRAY_MARKER);
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void emptyArrayRoot() {
      ArrayNode payload = objectMapper.createArrayNode();

      Throwable result = catchThrowable(() -> shredder.shred(payload, Collections.emptyList()));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_PUT_PAYLOAD_INVALID);
    }

    @Test
    public void withEmptyArray() {
      ObjectNode payload =
          objectMapper.createObjectNode().set("field", objectMapper.createArrayNode());

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isEqualTo(Constants.EMPTY_ARRAY_MARKER);
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void withArrayElements() {
      ObjectNode payload =
          objectMapper
              .createObjectNode()
              .set("field", objectMapper.createArrayNode().add("first").add("second"));

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .hasSize(2)
          .anySatisfy(
              row -> {
                assertThat(row.getPath()).containsExactly("field", "[000000]");
                assertThat(row.getLeaf()).isEqualTo("[000000]");
                assertThat(row.getStringValue()).isEqualTo("first");
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              })
          .anySatisfy(
              row -> {
                assertThat(row.getPath()).containsExactly("field", "[000001]");
                assertThat(row.getLeaf()).isEqualTo("[000001]");
                assertThat(row.getStringValue()).isEqualTo("second");
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void withArrayOverflow() {

      ObjectNode payload =
          objectMapper
              .createObjectNode()
              .set("field", objectMapper.createArrayNode().add("first").add("second").add("third"));

      Throwable result = catchThrowable(() -> shredder.shred(payload, Collections.emptyList()));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
    }

    @Test
    public void withEscapedFields() {
      ObjectNode payload = objectMapper.createObjectNode().put("period\\.", "text");

      List<JsonShreddedRow> result = shredder.shred(payload, Collections.emptyList());

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("period.");
                assertThat(row.getLeaf()).isEqualTo("period.");
                assertThat(row.getStringValue()).isEqualTo("text");
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void withInvalidFields() {
      ObjectNode payload = objectMapper.createObjectNode().put("period.", "text");

      Throwable result = catchThrowable(() -> shredder.shred(payload, Collections.emptyList()));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_INVALID_FIELD_NAME);
    }

    @Test
    public void withPrependPath() {
      ObjectNode payload = objectMapper.createObjectNode().put("field", "text");

      List<JsonShreddedRow> result = shredder.shred(payload, Arrays.asList("one", "two"));

      assertThat(result)
          .singleElement()
          .satisfies(
              row -> {
                assertThat(row.getPath()).containsExactly("one", "two", "field");
                assertThat(row.getLeaf()).isEqualTo("field");
                assertThat(row.getStringValue()).isEqualTo("text");
                assertThat(row.getDoubleValue()).isNull();
                assertThat(row.getBooleanValue()).isNull();
              });
    }

    @Test
    public void maxDepthExceeded() {
      ObjectNode payload = objectMapper.createObjectNode().put("field", "text");

      Throwable result =
          catchThrowable(() -> shredder.shred(payload, Arrays.asList("one", "two", "three")));

      assertThat(result)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }
  }
}
