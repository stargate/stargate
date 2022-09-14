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

package io.stargate.sgv2.docsapi.service.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocsApiUtilsTest {

  @Nested
  class PathToJsonPointer {

    @Test
    public void single() {
      String input = "path";

      Optional<JsonPointer> result = DocsApiUtils.pathToJsonPointer(input);

      AssertionsForClassTypes.assertThat(result)
          .hasValueSatisfying(
              jsonPointer -> {
                AssertionsForClassTypes.assertThat(jsonPointer.matchesProperty("path")).isTrue();
              });
    }

    @Test
    public void multi() {
      String input = "path.to.field";

      Optional<JsonPointer> result = DocsApiUtils.pathToJsonPointer(input);

      AssertionsForClassTypes.assertThat(result)
          .hasValueSatisfying(
              jsonPointer -> {
                AssertionsForClassTypes.assertThat(
                        jsonPointer
                            .matchProperty("path")
                            .matchProperty("to")
                            .matchesProperty("field"))
                    .isTrue();
              });
    }

    @Test
    public void arrayElement() {
      String input = "path.[115].field";

      Optional<JsonPointer> result = DocsApiUtils.pathToJsonPointer(input);

      AssertionsForClassTypes.assertThat(result)
          .hasValueSatisfying(
              jsonPointer -> {
                AssertionsForClassTypes.assertThat(
                        jsonPointer
                            .matchProperty("path")
                            .matchElement(115)
                            .matchesProperty("field"))
                    .isTrue();
              });
    }

    @Test
    public void arrayElementLiteral() {
      String input = "path.[0]";

      Optional<JsonPointer> result = DocsApiUtils.pathToJsonPointer(input);

      AssertionsForClassTypes.assertThat(result)
          .hasValueSatisfying(
              jsonPointer -> {
                AssertionsForClassTypes.assertThat(
                        jsonPointer.matchProperty("path").matchesElement(0))
                    .isTrue();
              });
    }

    @Test
    public void nullInput() {
      Optional<JsonPointer> result = DocsApiUtils.pathToJsonPointer(null);

      AssertionsForClassTypes.assertThat(result).isEmpty();
    }
  }

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
  class ConvertFieldsToPaths {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void happyPath() {
      ArrayNode input =
          objectMapper.createArrayNode().add("path.to.the.field").add("path.to.different");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input, 999999);
      AssertionsForInterfaceTypes.assertThat(result)
          .hasSize(2)
          .anySatisfy(
              l ->
                  AssertionsForInterfaceTypes.assertThat(l)
                      .containsExactly("path", "to", "the", "field"))
          .anySatisfy(
              l ->
                  AssertionsForInterfaceTypes.assertThat(l)
                      .containsExactly("path", "to", "different"));
    }

    @Test
    public void arrays() {
      ArrayNode input = objectMapper.createArrayNode().add("path.[*].any.[1].field");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input, 999999);
      AssertionsForInterfaceTypes.assertThat(result)
          .singleElement()
          .satisfies(
              l ->
                  AssertionsForInterfaceTypes.assertThat(l)
                      .containsExactly("path", "[*]", "any", "[000001]", "field"));
    }

    @Test
    public void segments() {
      ArrayNode input = objectMapper.createArrayNode().add("path.one,two.[0],[1].field");

      Collection<List<String>> result = DocsApiUtils.convertFieldsToPaths(input, 999999);
      AssertionsForInterfaceTypes.assertThat(result)
          .singleElement()
          .satisfies(
              l ->
                  AssertionsForInterfaceTypes.assertThat(l)
                      .containsExactly("path", "one,two", "[000000],[000001]", "field"));
    }

    @Test
    public void notArrayInput() {
      ObjectNode input = objectMapper.createObjectNode();

      Throwable t = catchThrowable(() -> DocsApiUtils.convertFieldsToPaths(input, 999999));

      AssertionsForClassTypes.assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID);
    }

    @Test
    public void containsNotStringInInput() {
      ArrayNode input = objectMapper.createArrayNode().add("path.one,two.[0],[1].field").add(15L);

      Throwable t = catchThrowable(() -> DocsApiUtils.convertFieldsToPaths(input, 999999));

      AssertionsForClassTypes.assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID);
    }
  }

  @Nested
  class GetFieldPath {
    @Test
    public void happyPath() {
      AssertionsForInterfaceTypes.assertThat(DocsApiUtils.getFieldPath("a.b.c", 64))
          .containsExactly("a", "b", "c");
      AssertionsForInterfaceTypes.assertThat(DocsApiUtils.getFieldPath("a.[0].c", 64))
          .containsExactly("a", "[000000]", "c");
      AssertionsForInterfaceTypes.assertThat(DocsApiUtils.getFieldPath("a", 64))
          .containsExactly("a");
    }

    @Test
    public void tooLong() {
      AssertionsForClassTypes.assertThatThrownBy(() -> DocsApiUtils.getFieldPath("a.[3].c", 2))
          .isInstanceOf(ErrorCodeRuntimeException.class);
    }
  }

  @Nested
  class ContainsIllegalSequences {
    @Test
    public void happyPath() {
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("[012]")).isTrue();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("aaa[012]"))
          .isTrue();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("]012[")).isTrue();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("[aaa]")).isTrue();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("[aaa")).isTrue();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("aaa]")).isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("a.2000")).isTrue();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("a\\.2000"))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("a'2000")).isTrue();
    }

    @Test
    public void happyPathDotsAllowed() {
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("[012]", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("aaa[012]", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("]012[", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("[aaa]", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("[aaa", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("aaa]", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("a.2000", true))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("a\\.2000"))
          .isFalse();
      AssertionsForClassTypes.assertThat(DocsApiUtils.containsIllegalSequences("a'2000")).isTrue();
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
  class ConvertJsonToBracketedPath {

    @Test
    public void happyPath() {
      String jsonPath = "$.a.b.c";

      String result = DocsApiUtils.convertJsonToBracketedPath(jsonPath);

      AssertionsForClassTypes.assertThat(result).isEqualTo("$['a']['b']['c']");
    }

    @Test
    public void withArrayElements() {
      String jsonPath = "$.a.b[2].c";

      String result = DocsApiUtils.convertJsonToBracketedPath(jsonPath);

      AssertionsForClassTypes.assertThat(result).isEqualTo("$['a']['b'][2]['c']");
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
  class BracketedPathAsArray {
    @Test
    public void happyPath() {
      String bracketedPath = "$['a']['b']['c']";

      String[] result = DocsApiUtils.bracketedPathAsArray(bracketedPath);

      AssertionsForClassTypes.assertThat(result.length).isEqualTo(3);
      AssertionsForClassTypes.assertThat(result[0]).isEqualTo("'a'");
      AssertionsForClassTypes.assertThat(result[1]).isEqualTo("'b'");
      AssertionsForClassTypes.assertThat(result[2]).isEqualTo("'c'");
    }

    @Test
    public void withArrayElements() {
      String bracketedPath = "$['a']['b'][2]['c']";

      String[] result = DocsApiUtils.bracketedPathAsArray(bracketedPath);

      AssertionsForClassTypes.assertThat(result.length).isEqualTo(4);
      AssertionsForClassTypes.assertThat(result[0]).isEqualTo("'a'");
      AssertionsForClassTypes.assertThat(result[1]).isEqualTo("'b'");
      AssertionsForClassTypes.assertThat(result[2]).isEqualTo("2");
      AssertionsForClassTypes.assertThat(result[3]).isEqualTo("'c'");
    }

    @Test
    public void singlePath() {
      String bracketedPath = "$['a']";

      String[] result = DocsApiUtils.bracketedPathAsArray(bracketedPath);

      AssertionsForClassTypes.assertThat(result.length).isEqualTo(1);
      AssertionsForClassTypes.assertThat(result[0]).isEqualTo("'a'");
    }
  }

  @Nested
  class GetStringFromRow {

    @Mock RowWrapper row;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    DocumentProperties documentProperties;

    @BeforeEach
    public void init() {
      when(documentProperties.tableProperties().stringValueColumnName()).thenReturn("text_value");
    }

    @Test
    public void happyPath() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(value);

      String result = DocsApiUtils.getStringFromRow(row, documentProperties);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void noValue() {
      when(row.isNull("text_value")).thenReturn(true);

      String result = DocsApiUtils.getStringFromRow(row, documentProperties);

      assertThat(result).isNull();
    }
  }

  @Nested
  class GetDoubleFromRow {

    @Mock RowWrapper row;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    DocumentProperties documentProperties;

    @BeforeEach
    public void init() {
      when(documentProperties.tableProperties().doubleValueColumnName()).thenReturn("dbl_value");
    }

    @Test
    public void happyPath() {
      Double value = RandomUtils.nextDouble();
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getDouble("dbl_value")).thenReturn(value);

      Double result = DocsApiUtils.getDoubleFromRow(row, documentProperties);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void noValue() {
      when(row.isNull("dbl_value")).thenReturn(true);

      Double result = DocsApiUtils.getDoubleFromRow(row, documentProperties);

      assertThat(result).isNull();
    }
  }

  @Nested
  class GetBooleanFromRow {

    @Mock RowWrapper row;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    DocumentProperties documentProperties;

    @BeforeEach
    public void init() {
      when(documentProperties.tableProperties().booleanValueColumnName()).thenReturn("bool_value");
    }

    @Test
    public void happyPath() {
      boolean value = RandomUtils.nextBoolean();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getBoolean("bool_value")).thenReturn(value);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, documentProperties, false);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void numericBooleansZero() {
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getByte("bool_value")).thenReturn((byte) 0);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, documentProperties, true);

      assertThat(result).isFalse();
    }

    @Test
    public void numericBooleansOne() {
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getByte("bool_value")).thenReturn((byte) 1);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, documentProperties, true);

      assertThat(result).isTrue();
    }

    @Test
    public void noValue() {
      when(row.isNull("bool_value")).thenReturn(true);

      Boolean result = DocsApiUtils.getBooleanFromRow(row, documentProperties, false);

      assertThat(result).isNull();
    }
  }
}
