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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
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
import io.stargate.web.docsapi.service.query.DocsApiConstants;
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

      String result = DocsApiUtils.convertArrayPath(String.format("[%d]", index), 999999);

      assertThat(result).isEqualTo(String.format("[000%d]", index));
    }

    @Test
    public void doubleConversionWorks() {
      int index = RandomUtils.nextInt(100, 999);
      String first = DocsApiUtils.convertArrayPath(String.format("[%d]", index), 999999);

      String result = DocsApiUtils.convertArrayPath(first, 999999);

      assertThat(result).isEqualTo(String.format("[000%d]", index));
    }

    @Test
    public void limitExceeded() {
      int index = RandomUtils.nextInt(100, 999);

      assertThatThrownBy(() -> DocsApiUtils.convertArrayPath(String.format("[%d]", index), 1))
          .hasMessageContaining("Max array length of 1 exceeded.");
    }

    @Test
    public void happyPathWithSegments() {
      String result = DocsApiUtils.convertArrayPath("[1],[44],[555]", 999999);

      assertThat(result).isEqualTo("[000001],[000044],[000555]");
    }

    @Test
    public void globIgnored() {
      String result = DocsApiUtils.convertArrayPath("[*]", 999999);

      assertThat(result).isEqualTo("[*]");
    }

    @Test
    public void notArrayIgnored() {
      String result = DocsApiUtils.convertArrayPath("somePath", 999999);

      assertThat(result).isEqualTo("somePath");
    }

    @Test
    public void notArrayWithSegmentsIgnored() {
      String result = DocsApiUtils.convertArrayPath("somePath,otherPath", 999999);

      assertThat(result).isEqualTo("somePath,otherPath");
    }

    @Test
    public void maxArrayExceeded() {
      Throwable t = catchThrowable(() -> DocsApiUtils.convertArrayPath("[1234567]", 999999));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED);
    }

    @Test
    public void numberFormatException() {
      Throwable t = catchThrowable(() -> DocsApiUtils.convertArrayPath("[not_allowed]", 999999));

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

      Optional<Integer> result =
          DocsApiUtils.extractArrayPathIndex(String.format("[%d]", index), 999999);

      assertThat(result).hasValue(index);
    }

    @Test
    public void emptyNotArray() {
      Optional<Integer> result = DocsApiUtils.extractArrayPathIndex("some", 999999);

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class IsArrayPath {

    @Test
    public void notConverted() {
      boolean result = DocsApiUtils.isArrayPath("[5]");

      assertThat(result).isTrue();
    }

    @Test
    public void alreadyConverted() {
      boolean result = DocsApiUtils.isArrayPath("[000123]");

      assertThat(result).isTrue();
    }

    @Test
    public void notArray() {
      boolean result = DocsApiUtils.isArrayPath("whooo");

      assertThat(result).isFalse();
    }
  }

  @Nested
  class ConvertEscapedCharacters {

    @Test
    public void happyPath() {
      String result = DocsApiUtils.convertEscapedCharacters("\\. is a period");

      assertThat(result).isEqualTo(". is a period");

      result = DocsApiUtils.convertEscapedCharacters("I can represent asterisks too: \\*");
      assertThat(result).isEqualTo("I can represent asterisks too: *");

      result = DocsApiUtils.convertEscapedCharacters("I can represent commas too: \\,");
      assertThat(result).isEqualTo("I can represent commas too: ,");

      result =
          DocsApiUtils.convertEscapedCharacters(
              "\\ but without valid chars after are ignored: \\a");
      assertThat(result).isEqualTo("\\ but without valid chars after are ignored: \\a");
    }

    @Test
    public void asList() {
      List<String> converted =
          DocsApiUtils.convertEscapedCharacters(
              Arrays.asList("\\. is a period", "I can represent asterisks too: \\*"));
      assertThat(converted)
          .isEqualTo(Arrays.asList(". is a period", "I can represent asterisks too: *"));
    }
  }

  @Nested
  class ConvertFieldsToPaths {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void happyPath() {
      ArrayNode input =
          objectMapper.createArrayNode().add("path.to.the.field").add("path.to.different");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input, 999999);
      assertThat(result)
          .hasSize(2)
          .anySatisfy(l -> assertThat(l).containsExactly("path", "to", "the", "field"))
          .anySatisfy(l -> assertThat(l).containsExactly("path", "to", "different"));
    }

    @Test
    public void arrays() {
      ArrayNode input = objectMapper.createArrayNode().add("path.[*].any.[1].field");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input, 999999);
      assertThat(result)
          .singleElement()
          .satisfies(l -> assertThat(l).containsExactly("path", "[*]", "any", "[000001]", "field"));
    }

    @Test
    public void segments() {
      ArrayNode input = objectMapper.createArrayNode().add("path.one,two.[0],[1].field");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input, 999999);
      assertThat(result)
          .singleElement()
          .satisfies(
              l -> assertThat(l).containsExactly("path", "one,two", "[000000],[000001]", "field"));
    }

    @Test
    public void notArrayInput() {
      ObjectNode input = objectMapper.createObjectNode();

      Throwable t = catchThrowable(() -> DocsApiUtils.convertFieldsToPaths(input, 999999));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID);
    }

    @Test
    public void containsNotStringInInput() {
      ArrayNode input = objectMapper.createArrayNode().add("path.one,two.[0],[1].field").add(15L);

      Throwable t = catchThrowable(() -> DocsApiUtils.convertFieldsToPaths(input, 999999));

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
  class BracketedPathAsArray {
    @Test
    public void happyPath() {
      String bracketedPath = "$['a']['b']['c']";

      String[] result = DocsApiUtils.bracketedPathAsArray(bracketedPath);

      assertThat(result.length).isEqualTo(3);
      assertThat(result[0]).isEqualTo("'a'");
      assertThat(result[1]).isEqualTo("'b'");
      assertThat(result[2]).isEqualTo("'c'");
    }

    @Test
    public void withArrayElements() {
      String bracketedPath = "$['a']['b'][2]['c']";

      String[] result = DocsApiUtils.bracketedPathAsArray(bracketedPath);

      assertThat(result.length).isEqualTo(4);
      assertThat(result[0]).isEqualTo("'a'");
      assertThat(result[1]).isEqualTo("'b'");
      assertThat(result[2]).isEqualTo("2");
      assertThat(result[3]).isEqualTo("'c'");
    }

    @Test
    public void singlePath() {
      String bracketedPath = "$['a']";

      String[] result = DocsApiUtils.bracketedPathAsArray(bracketedPath);

      assertThat(result.length).isEqualTo(1);
      assertThat(result[0]).isEqualTo("'a'");
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
  class ContainsIllegalSequences {
    @Test
    public void happyPath() {
      assertThat(DocsApiUtils.containsIllegalSequences("[012]")).isTrue();
      assertThat(DocsApiUtils.containsIllegalSequences("aaa[012]")).isTrue();
      assertThat(DocsApiUtils.containsIllegalSequences("]012[")).isTrue();
      assertThat(DocsApiUtils.containsIllegalSequences("[aaa]")).isTrue();
      assertThat(DocsApiUtils.containsIllegalSequences("[aaa")).isTrue();
      assertThat(DocsApiUtils.containsIllegalSequences("aaa]")).isFalse();
      assertThat(DocsApiUtils.containsIllegalSequences("a.2000")).isTrue();
      assertThat(DocsApiUtils.containsIllegalSequences("a\\.2000")).isFalse();
      assertThat(DocsApiUtils.containsIllegalSequences("a'2000")).isTrue();
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
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "value",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "value",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  ""));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void matchingSpecialCharacters() {
      List<String> path = Arrays.asList("field\\,with", "commas\\.");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "commas.",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field,with",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "commas.",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
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
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "value",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "value",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  "value",
                  DocsApiConstants.P_COLUMN_NAME.apply(3),
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
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "other",
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "value"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isTrue();
    }

    @Test
    public void matchingSpecialCharacters() {
      List<String> path = Arrays.asList("field\\,with", "commas\\.");
      Row row =
          MapBackedRow.of(
              TABLE,
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field,with",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "commas."));

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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "parent",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
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
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  "field",
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  "value"));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      Assertions.assertThat(result).isFalse();
    }
  }
}
