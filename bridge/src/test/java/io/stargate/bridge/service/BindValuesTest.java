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
package io.stargate.bridge.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.grpc.StatusException;
import io.stargate.bridge.Utils;
import io.stargate.bridge.codec.ValueCodecs;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.QueryOuterClass.Value.Null;
import io.stargate.bridge.proto.QueryOuterClass.Value.Unset;
import io.stargate.bridge.proto.QueryOuterClass.Values;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BindValuesTest {
  @ParameterizedTest
  @MethodSource({"validPayloads", "emptyPayload", "unsetAndNullPayloads"})
  public void bindValuesValid(Prepared prepared, Values values) throws Exception {
    BoundStatement statement = ValuesHelper.bindValues(prepared, values, Utils.UNSET);

    validate(statement, prepared, values);
  }

  public static Stream<Arguments> validPayloads() {
    return Stream.of(
        arguments(Utils.makePrepared(), Values.newBuilder().build()),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder()
                .addValues(Value.newBuilder().setInt(123).build())
                .addValues(Value.newBuilder().setString("abc").build())
                .build()),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder()
                .addValueNames("v1")
                .addValues(Value.newBuilder().setInt(123).build())
                .addValueNames("v2")
                .addValues(Value.newBuilder().setString("abc").build())
                .build()));
  }

  public static Stream<Arguments> emptyPayload() {
    return Stream.of(arguments(Utils.makePrepared(), Values.newBuilder().build()));
  }

  public static Stream<Arguments> unsetAndNullPayloads() {
    return Stream.of(
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder()
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .addValues(Value.newBuilder().setUnset(Unset.newBuilder().build()).build())
                .build()),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder()
                .addValueNames("v1")
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .addValueNames("v2")
                .addValues(Value.newBuilder().setUnset(Unset.newBuilder().build()).build())
                .build()));
  }

  @ParameterizedTest
  @MethodSource({"invalidArityPayloads", "invalidNamesPayloads", "invalidTypePayloads"})
  public void bindValuesInvalid(
      Prepared prepared, Values values, Class<?> expectedException, String expectedMessage) {
    assertThatThrownBy(
            () -> {
              ValuesHelper.bindValues(prepared, values, Utils.UNSET);
            })
        .isInstanceOf(expectedException)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidArityPayloads() {
    return Stream.of(
        arguments(
            Utils.makePrepared(),
            Values.newBuilder()
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .build(),
            StatusException.class,
            "Invalid number of bind values. Expected 0, but received 1"),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder().build(),
            StatusException.class,
            "Invalid number of bind values. Expected 2, but received 0"),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder()
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .build(),
            StatusException.class,
            "Invalid number of bind values. Expected 2, but received 3"));
  }

  public static Stream<Arguments> invalidNamesPayloads() {
    return Stream.of(
        arguments(
            Utils.makePrepared(),
            Values.newBuilder().addValueNames("v1").build(),
            StatusException.class,
            "Invalid number of bind names. Expected 0, but received 1"),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int)),
            Values.newBuilder()
                .addValueNames("v1")
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .addValueNames("v2")
                .build(),
            StatusException.class,
            "Invalid number of bind names. Expected 1, but received 2"),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int)),
            Values.newBuilder()
                .addValueNames("doesNotExist")
                .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                .build(),
            StatusException.class,
            "Unable to find bind marker with name 'doesNotExist'"));
  }

  public static Stream<Arguments> invalidTypePayloads() {
    return Stream.of(
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int)),
            Values.newBuilder()
                .addValues(Value.newBuilder().setString("notAnInteger").build())
                .build(),
            StatusException.class,
            "Invalid argument at position 1"),
        arguments(
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            Values.newBuilder()
                .addValues(Value.newBuilder().setInt(123).build())
                .addValues(Value.newBuilder().setInt(456).build())
                .build(),
            StatusException.class,
            "Invalid argument at position 2"));
  }

  private static void validate(BoundStatement statement, Prepared prepared, Values values) {
    assertThat(values.getValuesCount()).isEqualTo(statement.values().size());

    if (values.getValueNamesCount() > 0) {
      assertThat(statement.boundNames()).isPresent();
      statement
          .boundNames()
          .ifPresent(
              names -> {
                assertThat(names).isEqualTo(values.getValueNamesList());
                List<Column> columns = prepared.metadata.columns;
                for (String name : names) {
                  int index =
                      IntStream.of(0, columns.size() - 1)
                          .filter(i -> columns.get(i).name().equals(name))
                          .findFirst()
                          .orElseThrow(() -> new AssertionError("Unable to find column"));
                  Column column = columns.get(index);
                  Value decodedValue = decodeValue(column.type(), statement.values().get(index));
                  assertThat(decodedValue).isEqualTo(values.getValues(index));
                }
              });
    } else {
      List<Column> columns = prepared.metadata.columns;
      for (int i = 0; i < columns.size(); ++i) {
        Column column = columns.get(i);
        Value decodedValue = decodeValue(column.type(), statement.values().get(i));
        assertThat(decodedValue).isEqualTo(values.getValues(i));
      }
    }
  }

  private static Value decodeValue(Column.ColumnType type, ByteBuffer bytes) {
    if (bytes == null) {
      return Value.newBuilder().setNull(Null.newBuilder().build()).build();
    } else if (bytes == Utils.UNSET) {
      return Value.newBuilder().setUnset(Unset.newBuilder().build()).build();
    } else {
      return ValueCodecs.get(type.rawType()).decode(bytes, type);
    }
  }
}
