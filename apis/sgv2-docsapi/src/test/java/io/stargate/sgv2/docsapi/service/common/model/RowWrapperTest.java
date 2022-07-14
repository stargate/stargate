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

package io.stargate.sgv2.docsapi.service.common.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RowWrapperTest {

  @Nested
  class ColumnExists {

    @Test
    public void happyPath() {
      String name = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(name).build();
      QueryOuterClass.Row row = QueryOuterClass.Row.newBuilder().build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      boolean result = wrapper.columnExists(name);

      assertThat(result).isTrue();
    }

    @Test
    public void notExisting() {
      String name = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(name).build();
      QueryOuterClass.Row row = QueryOuterClass.Row.newBuilder().build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      boolean result = wrapper.columnExists("some");

      assertThat(result).isFalse();
    }
  }

  @Nested
  class ColumnIndexMap {

    @Test
    public void happyPath() {
      String name = RandomStringUtils.randomAlphabetic(16);
      String name2 = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(name).build();
      QueryOuterClass.ColumnSpec columnSpec2 =
          QueryOuterClass.ColumnSpec.newBuilder().setName(name2).build();
      QueryOuterClass.Row row = QueryOuterClass.Row.newBuilder().build();

      RowWrapper wrapper = RowWrapper.forColumns(Arrays.asList(columnSpec, columnSpec2)).apply(row);
      Map<String, Integer> result = wrapper.columnIndexMap();

      assertThat(result).hasSize(2).containsEntry(name, 0).containsEntry(name2, 1);
    }
  }

  @Nested
  class IsNull {

    @Test
    public void nullValue() {
      String column = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row = QueryOuterClass.Row.newBuilder().addValues(Values.NULL).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      boolean result = wrapper.isNull(column);

      assertThat(result).isTrue();
    }

    @Test
    public void notNullValue() {
      String column = RandomStringUtils.randomAlphabetic(16);
      String value = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row =
          QueryOuterClass.Row.newBuilder().addValues(Values.of(value)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      boolean result = wrapper.isNull(column);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class GetString {

    @Test
    public void happyPath() {
      String column = RandomStringUtils.randomAlphabetic(16);
      String value = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row =
          QueryOuterClass.Row.newBuilder().addValues(Values.of(value)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      String result = wrapper.getString(column);

      assertThat(result).isEqualTo(value);
    }

    @Test
    public void notExistingColumn() {
      String column = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row = QueryOuterClass.Row.newBuilder().build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      Throwable throwable = catchThrowable(() -> wrapper.getString("some"));

      assertThat(throwable)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Column 'some' does not exist");
    }

    @Test
    public void notStringColumn() {
      String column = RandomStringUtils.randomAlphabetic(16);
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row = QueryOuterClass.Row.newBuilder().addValues(Values.of(0)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      Throwable throwable = catchThrowable(() -> wrapper.getString(column));

      assertThat(throwable)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Expected STRING value, received INT");
    }
  }

  @Nested
  class GetDouble {

    @Test
    public void happyPath() {
      String column = RandomStringUtils.randomAlphabetic(16);
      double value = RandomUtils.nextDouble();
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row =
          QueryOuterClass.Row.newBuilder().addValues(Values.of(value)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      double result = wrapper.getDouble(column);

      assertThat(result).isEqualTo(value);
    }
  }

  @Nested
  class GetLong {

    @Test
    public void happyPath() {
      String column = RandomStringUtils.randomAlphabetic(16);
      long value = RandomUtils.nextLong();
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row =
          QueryOuterClass.Row.newBuilder().addValues(Values.of(value)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      long result = wrapper.getLong(column);

      assertThat(result).isEqualTo(value);
    }
  }

  @Nested
  class GetByte {

    @Test
    public void happyPath() {
      String column = RandomStringUtils.randomAlphabetic(16);
      byte value = RandomUtils.nextBytes(1)[0];
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row =
          QueryOuterClass.Row.newBuilder().addValues(Values.of(value)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      byte result = wrapper.getByte(column);

      assertThat(result).isEqualTo(value);
    }
  }

  @Nested
  class GetBoolean {

    @Test
    public void happyPath() {
      String column = RandomStringUtils.randomAlphabetic(16);
      boolean value = RandomUtils.nextBoolean();
      QueryOuterClass.ColumnSpec columnSpec =
          QueryOuterClass.ColumnSpec.newBuilder().setName(column).build();
      QueryOuterClass.Row row =
          QueryOuterClass.Row.newBuilder().addValues(Values.of(value)).build();

      RowWrapper wrapper = RowWrapper.forColumns(Collections.singletonList(columnSpec)).apply(row);
      boolean result = wrapper.getBoolean(column);

      assertThat(result).isEqualTo(value);
    }
  }
}
