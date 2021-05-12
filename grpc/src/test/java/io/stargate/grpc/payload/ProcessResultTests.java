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
package io.stargate.grpc.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.google.protobuf.Any;
import io.stargate.db.Result.Rows;
import io.stargate.db.schema.Column;
import io.stargate.grpc.Utils;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ProcessResultTests {
  @ParameterizedTest
  @MethodSource("results")
  public void processResult(
      Payload.Type type, Rows rows, QueryParameters queryParameters, Payload expected)
      throws Exception {

    PayloadHandler handler = PayloadHandlers.get(type);
    Payload actual = handler.processResult(rows, queryParameters);
    assertThat(actual).isEqualTo(expected);
  }

  public static Stream<Arguments> results() {
    return Stream.of(
        resultSet()
            .addActualColumn(Column.create("c1", Column.Type.Int))
            .addActualColumn(Column.create("c2", Column.Type.Varchar))
            .addActualColumn(Column.create("c3", Column.Type.Uuid))
            .addExpectedColumn(
                ColumnSpec.newBuilder()
                    .setName("c1")
                    .setType(TypeSpec.newBuilder().setType(TypeSpec.Type.TYPE_INT)))
            .addExpectedColumn(
                ColumnSpec.newBuilder()
                    .setName("c2")
                    .setType(TypeSpec.newBuilder().setType(TypeSpec.Type.TYPE_VARCHAR)))
            .addExpectedColumn(
                ColumnSpec.newBuilder()
                    .setName("c3")
                    .setType(TypeSpec.newBuilder().setType(TypeSpec.Type.TYPE_UUID)))
            .addActualRow(1, "a", UUID.fromString("d1dbc5ca-b4e9-43ec-9ffd-e5bada9dc531"))
            .addActualRow(2, "b", UUID.fromString("f09f1429-05d1-4dd3-98fc-a5324ebcb113"))
            .addExpectedRow(
                Values.of(1),
                Values.of("a"),
                Values.of(UUID.fromString("d1dbc5ca-b4e9-43ec-9ffd-e5bada9dc531")))
            .addExpectedRow(
                Values.of(2),
                Values.of("b"),
                Values.of(UUID.fromString("f09f1429-05d1-4dd3-98fc-a5324ebcb113")))
            .build(false),
        resultSet()
            .addActualColumn(Column.create("c1", Column.Type.Int))
            .addActualColumn(Column.create("c2", Column.Type.Varchar))
            .addActualColumn(Column.create("c3", Column.Type.Uuid))
            .addActualRow(1, "a", UUID.fromString("d1dbc5ca-b4e9-43ec-9ffd-e5bada9dc531"))
            .addActualRow(2, "b", UUID.fromString("f09f1429-05d1-4dd3-98fc-a5324ebcb113"))
            .addExpectedRow(
                Values.of(1),
                Values.of("a"),
                Values.of(UUID.fromString("d1dbc5ca-b4e9-43ec-9ffd-e5bada9dc531")))
            .addExpectedRow(
                Values.of(2),
                Values.of("b"),
                Values.of(UUID.fromString("f09f1429-05d1-4dd3-98fc-a5324ebcb113")))
            .build(true));
  }

  private static class ResultSetBuilder {
    private final List<Column> columns = new ArrayList<>();
    private final List<List<ByteBuffer>> rows = new ArrayList<>();
    private final ResultSet.Builder resultSet = ResultSet.newBuilder();

    public ResultSetBuilder addExpectedColumn(ColumnSpec.Builder columnSpec) {
      resultSet.addColumns(columnSpec.build());
      return this;
    }

    public ResultSetBuilder addActualColumn(Column column) {
      columns.add(column);
      return this;
    }

    public ResultSetBuilder addExpectedRow(Value... values) {
      assertThat(columns).hasSize(values.length);
      resultSet.addRows(Row.newBuilder().addAllValues(Arrays.asList(values)));
      return this;
    }

    public ResultSetBuilder addActualRow(Object... values) {
      assertThat(columns).hasSize(values.length);
      List<ByteBuffer> row = new ArrayList<>(values.length);
      for (int i = 0; i < columns.size(); ++i) {
        Column column = columns.get(i);
        row.add(column.type().codec().encode(values[i], ProtocolVersion.DEFAULT));
      }
      rows.add(row);
      return this;
    }

    Arguments build(boolean skipMetadata) {
      return arguments(
          Type.TYPE_CQL,
          new Rows(rows, Utils.makeResultMetadata(columns.toArray(new Column[columns.size()]))),
          QueryParameters.newBuilder().setSkipMetadata(skipMetadata).build(),
          Payload.newBuilder()
              .setType(Type.TYPE_CQL)
              .setValue(Any.pack(resultSet.build()))
              .build());
    }
  }

  private static ResultSetBuilder resultSet() {
    return new ResultSetBuilder();
  }
}
