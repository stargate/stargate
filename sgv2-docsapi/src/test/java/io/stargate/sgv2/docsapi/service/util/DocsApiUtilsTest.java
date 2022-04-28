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

import com.google.common.collect.ImmutableMap;
import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.DocsApiConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
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
  class GetStringFromRow {

    @Mock RowWrapper row;

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

    @Mock RowWrapper row;

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

    @Mock RowWrapper row;

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

    @Test
    public void matchingExact() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("value"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("value"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSpecialCharacters() {
      List<String> path = Arrays.asList("field\\,with", "commas\\.");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("commas."),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field,with"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("commas."),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void notMatchingExtraDepth() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("value"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("value"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("value"),
                  DocsApiConstants.P_COLUMN_NAME.apply(3),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      assertThat(result).isFalse();
    }

    @Test
    public void notMatchingWrongField() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.LEAF_COLUMN_NAME,
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("other"),
                  DocsApiConstants.P_COLUMN_NAME.apply(2),
                  Values.of("")));

      boolean result = DocsApiUtils.isRowMatchingPath(row, path);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class IsRowOnPath {

    private final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);

    @Test
    public void matchingExact() {
      List<String> path = Arrays.asList("field", "value");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("value")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSpecialCharacters() {
      List<String> path = Arrays.asList("field\\,with", "commas\\.");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field,with"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("commas.")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSubPath() {
      List<String> path = Collections.singletonList("field");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("more")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingSegment() {
      List<String> path = Arrays.asList("field", "value1,value2");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("value2")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingGlob() {
      List<String> path = Arrays.asList("*", "[*]");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("[000001]")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void pathNotMatchingDifferent() {
      List<String> path = Arrays.asList("parent", "field");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("parent")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isFalse();
    }

    @Test
    public void pathNotMatchingTooShort() {
      List<String> path = Arrays.asList("parent", "field");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("parent"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isFalse();
    }

    @Test
    public void notMatchingSegment() {
      List<String> path = Arrays.asList("field", "value1,value2");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("noValue")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isFalse();
    }

    @Test
    public void matchingMatchingGlobArray() {
      List<String> path = Arrays.asList("field", "*");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0), Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1), Values.of("[000001]")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isTrue();
    }

    @Test
    public void matchingMatchingGlob() {
      List<String> path = Arrays.asList("field", "[*]");
      RowWrapper row =
          SCHEMA_PROVIDER.getRow(
              ImmutableMap.of(
                  DocsApiConstants.P_COLUMN_NAME.apply(0),
                  Values.of("field"),
                  DocsApiConstants.P_COLUMN_NAME.apply(1),
                  Values.of("value")));

      boolean result = DocsApiUtils.isRowOnPath(row, path);

      assertThat(result).isFalse();
    }
  }
}
