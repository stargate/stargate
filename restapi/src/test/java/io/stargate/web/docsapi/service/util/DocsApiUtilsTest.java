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

import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.db.datastore.MapBackedRow;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.QueryConstants;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocsApiUtilsTest {

  @Nested
  class ConvertArrayPath {

    @Test
    public void happyPath() {
      int index = RandomUtils.nextInt(100, 999);

      String result = DocsApiUtils.convertArrayPath(String.format("[%d]", index));

      assertThat(result).isEqualTo(String.format("[000%d]", index));
    }

    @Test
    public void happyPathWithSegments() {
      String result = DocsApiUtils.convertArrayPath("[1],[44],[555]");

      assertThat(result).isEqualTo("[000001],[000044],[000555]");
    }

    @Test
    public void globIgnored() {
      String result = DocsApiUtils.convertArrayPath("[*]");

      assertThat(result).isEqualTo("[*]");
    }

    @Test
    public void notArrayIgnored() {
      String result = DocsApiUtils.convertArrayPath("somePath");

      assertThat(result).isEqualTo("somePath");
    }

    @Test
    public void notArrayWithSegmentsIgnored() {
      String result = DocsApiUtils.convertArrayPath("somePath,otherPath");

      assertThat(result).isEqualTo("somePath,otherPath");
    }

    @Test
    public void maxArrayExceeded() {
      Throwable t = catchThrowable(() -> DocsApiUtils.convertArrayPath("[1234567]"));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
    }

    @Test
    public void numberFormatException() {
      Throwable t = catchThrowable(() -> DocsApiUtils.convertArrayPath("[not_allowed]"));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID);
    }
  }

  @Nested
  class ExtractArrayPathIndex {

    @Test
    public void happyPath() {
      int index = RandomUtils.nextInt(100, 999);

      Optional<Integer> result = DocsApiUtils.extractArrayPathIndex(String.format("[%d]", index));

      assertThat(result).hasValue(index);
    }

    @Test
    public void emptyNotArray() {
      Optional<Integer> result = DocsApiUtils.extractArrayPathIndex("some");

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class ConvertUnicodeCodePoints {

    @Test
    public void happyPath() {
      String result = DocsApiUtils.convertUnicodeCodePoints("\\u002E is a period");

      assertThat(result).isEqualTo(". is a period");

      result =
          DocsApiUtils.convertUnicodeCodePoints("I can represent braces too: \\u005b000\\u005d");
      assertThat(result).isEqualTo("I can represent braces too: [000]");

      result =
          DocsApiUtils.convertUnicodeCodePoints(
              "\\u but without valid code points after are ignored: \\ufg00");
      assertThat(result).isEqualTo("\\u but without valid code points after are ignored: \\ufg00");
    }
  }

  @Nested
  class ConvertFieldsToPaths {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void happyPath() {
      ArrayNode input =
          objectMapper.createArrayNode().add("path.to.the.field").add("path.to.different");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input);
      assertThat(result)
          .hasSize(2)
          .anySatisfy(l -> assertThat(l).containsExactly("path", "to", "the", "field"))
          .anySatisfy(l -> assertThat(l).containsExactly("path", "to", "different"));
    }

    @Test
    public void arrays() {
      ArrayNode input = objectMapper.createArrayNode().add("path.[*].any.[1].field");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input);
      assertThat(result)
          .singleElement()
          .satisfies(l -> assertThat(l).containsExactly("path", "[*]", "any", "[000001]", "field"));
    }

    @Test
    public void segments() {
      ArrayNode input = objectMapper.createArrayNode().add("path.one,two.[0],[1].field");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input);
      assertThat(result)
          .singleElement()
          .satisfies(
              l -> assertThat(l).containsExactly("path", "one,two", "[000000],[000001]", "field"));
    }

    @Test
    public void notArrayInput() {
      ObjectNode input = objectMapper.createObjectNode();

      Throwable t = catchThrowable(() -> DocsApiUtils.convertFieldsToPaths(input));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID);
    }

    @Test
    public void containsNotStringInInput() {
      ArrayNode input = objectMapper.createArrayNode().add("path.one,two.[0],[1].field").add(15L);

      Throwable t = catchThrowable(() -> DocsApiUtils.convertFieldsToPaths(input));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID);
    }
  }

  @Nested
  class ConvertJsonToBracketedPath {

    @Test
    public void happyPath() {
      String jsonPath = "$.a.b.c";

      String result = DocsApiUtils.convertJsonToBracketedPath(jsonPath);

      assertThat(result).isEqualTo("$['a']['b']['c']");
    }

    @Test
    public void withArrayElements() {
      String jsonPath = "$.a.b[2].c";

      String result = DocsApiUtils.convertJsonToBracketedPath(jsonPath);

      assertThat(result).isEqualTo("$['a']['b'][2]['c']");
    }
  }

  @Nested
  class GetStringFromRow {

    @Mock Row row;

    @Test
    public void happyPath() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(value);

      String result = DocsApiUtils.getStringFromRow(row);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void noValue() {
      when(row.isNull("text_value")).thenReturn(true);

      String result = DocsApiUtils.getStringFromRow(row);

      assertThat(result).isNull();
    }
  }

  @Nested
  class GetDoubleFromRow {

    @Mock Row row;

    @Test
    public void happyPath() {
      Double value = RandomUtils.nextDouble();
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getDouble("dbl_value")).thenReturn(value);

      Double result = DocsApiUtils.getDoubleFromRow(row);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void noValue() {
      when(row.isNull("dbl_value")).thenReturn(true);

      Double result = DocsApiUtils.getDoubleFromRow(row);

      assertThat(result).isNull();
    }
  }

  @Nested
  class GetBooleanFromRow {

    @Mock Row row;

    @Test
    public void happyPath() {
      boolean value = RandomUtils.nextBoolean();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getBoolean("bool_value")).thenReturn(value);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, false);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void numericBooleansZero() {
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getByte("bool_value")).thenReturn((byte) 0);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, true);

      assertThat(result).isFalse();
    }

    @Test
    public void numericBooleansOne() {
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getByte("bool_value")).thenReturn((byte) 1);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, true);

      assertThat(result).isTrue();
    }

    @Test
    public void noValue() {
      when(row.isNull("bool_value")).thenReturn(true);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, false);

      assertThat(result).isNull();
    }
  }

  @Nested
  class IsRowMatchingPath {

    private final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
    private final Table TABLE = SCHEMA_PROVIDER.getTable();

    @Test
    public void matchingExact() {
      List<String> path = Arrays.asList("field", "value");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.LEAF_COLUMN_NAME,
                  "value",
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "value",
                  QueryConstants.P_COLUMN_NAME.apply(2),
                  ""));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void notMatchingExtraDepth() {
      List<String> path = Arrays.asList("field", "value");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.LEAF_COLUMN_NAME,
                  "value",
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "value",
                  QueryConstants.P_COLUMN_NAME.apply(2),
                  "value",
                  QueryConstants.P_COLUMN_NAME.apply(3),
                  ""));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      Assertions.assertThat(result).isFalse();
    }

    @Test
    public void notMatchingWrongField() {
      List<String> path = Arrays.asList("field", "value");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.LEAF_COLUMN_NAME,
                  "other",
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "other",
                  QueryConstants.P_COLUMN_NAME.apply(2),
                  ""));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      Assertions.assertThat(result).isFalse();
    }
  }

  @Nested
  class IsRowOnPath {

    private final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
    private final Table TABLE = SCHEMA_PROVIDER.getTable();

    @Test
    public void matchingExact() {
      List<String> path = Arrays.asList("field", "value");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "value"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void matchingSubPath() {
      List<String> path = Collections.singletonList("field");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "more"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void matchingSegment() {
      List<String> path = Arrays.asList("field", "value1,value2");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "value2"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void matchingGlob() {
      List<String> path = Arrays.asList("*", "[*]");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "[000001]"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void pathNotMatchingDifferent() {
      List<String> path = Arrays.asList("parent", "field");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "parent"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isFalse();
    }

    @Test
    public void pathNotMatchingTooShort() {
      List<String> path = Arrays.asList("parent", "field");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  ""));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isFalse();
    }

    @Test
    public void notMatchingSegment() {
      List<String> path = Arrays.asList("field", "value1,value2");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "noValue"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isFalse();
    }

    @Test
    public void matchingMatchingGlobArray() {
      List<String> path = Arrays.asList("field", "*");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "[000001]"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void matchingMatchingGlob() {
      List<String> path = Arrays.asList("field", "[*]");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  QueryConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  QueryConstants.P_COLUMN_NAME.apply(1),
                  "value"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isFalse();
    }
  }
}
