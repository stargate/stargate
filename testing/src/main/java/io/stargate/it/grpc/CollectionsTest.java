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
package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS list_test (k text, v list<text>, PRIMARY KEY(k))",
      "CREATE TABLE IF NOT EXISTS set_test (k text, v set<text>, PRIMARY KEY(k))",
      "CREATE TABLE IF NOT EXISTS map_test (k text, v map<text, int>, PRIMARY KEY(k))",
      "CREATE TABLE IF NOT EXISTS tuple_test (k text, v tuple<text, int, uuid>, PRIMARY KEY(k))",
    })
public class CollectionsTest extends GrpcIntegrationTest {

  @ParameterizedTest
  @MethodSource("collectionValues")
  public void collections(Value collection, String tableName, @TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery(
                String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                queryParameters(keyspace),
                Values.of("a"),
                collection));
    assertThat(response).isNotNull();

    response =
        stub.executeQuery(
            cqlQuery(
                String.format("SELECT * FROM %s WHERE k = ?", tableName),
                queryParameters(keyspace),
                Values.of("a")));
    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(2);
    assertThat(rs.getRows(0).getValues(0)).isEqualTo(Values.of("a"));
    assertThat(rs.getRows(0).getValues(1)).isEqualTo(collection);
  }

  public static Stream<Arguments> collectionValues() {
    return Stream.of(
        Arguments.of(Values.of(Values.of("a"), Values.of("b"), Values.of("c")), "list_test"),
        Arguments.of(Values.of(Values.of("a"), Values.of("b"), Values.of("c")), "set_test"),
        Arguments.of(
            Values.of(
                Values.of("a"), Values.of(1),
                Values.of("b"), Values.of(2),
                Values.of("c"), Values.of(3)),
            "map_test"),
        Arguments.of(Values.of(Values.of("a")), "tuple_test"),
        Arguments.of(Values.of(Values.of("a"), Values.of(1)), "tuple_test"),
        Arguments.of(
            Values.of(Values.of("a"), Values.of(1), Values.of(UUID.randomUUID())), "tuple_test"),
        Arguments.of(
            Values.of(Values.NULL, Values.of(1), Values.of(UUID.randomUUID())), "tuple_test"),
        Arguments.of(
            Values.of(Values.of("a"), Values.NULL, Values.of(UUID.randomUUID())), "tuple_test"),
        Arguments.of(Values.of(Values.of("a"), Values.of(1), Values.NULL), "tuple_test"),
        Arguments.of(Values.of(Values.NULL, Values.NULL, Values.NULL), "tuple_test"));
  }

  @ParameterizedTest
  @MethodSource("emptyValues")
  public void emptyCollections(
      Value collection, Value expected, String tableName, @TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery(
                String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                queryParameters(keyspace),
                Values.of("b"),
                collection));
    assertThat(response).isNotNull();

    response =
        stub.executeQuery(
            cqlQuery(
                String.format("SELECT * FROM %s WHERE k = ?", tableName),
                queryParameters(keyspace),
                Values.of("b")));
    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(2);
    assertThat(rs.getRows(0).getValues(0)).isEqualTo(Values.of("b"));
    assertThat(rs.getRows(0).getValues(1)).isEqualTo(expected);
  }

  public static Stream<Arguments> emptyValues() {
    return Stream.of(
        Arguments.of(Values.of(), Values.NULL, "list_test"),
        Arguments.of(Values.NULL, Values.NULL, "list_test"),
        Arguments.of(Values.of(), Values.NULL, "set_test"),
        Arguments.of(Values.NULL, Values.NULL, "set_test"),
        Arguments.of(Values.of(), Values.NULL, "map_test"),
        Arguments.of(Values.NULL, Values.NULL, "map_test"),
        Arguments.of(Values.of(), Values.of(), "tuple_test"),
        Arguments.of(Values.NULL, Values.NULL, "tuple_test"));
  }

  @ParameterizedTest
  @MethodSource("invalidValues")
  public void invalidCollections(
      Value collection,
      String tableName,
      String expectedMessage,
      @TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    assertThatThrownBy(
            () -> {
              Response response =
                  stub.executeQuery(
                      cqlQuery(
                          String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                          queryParameters(keyspace),
                          Values.of("b"),
                          collection));
              assertThat(response).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        Arguments.of(
            Values.of(Values.of(1)),
            "list_test",
            "Invalid argument at position 2: Expected string type"),
        Arguments.of(
            Values.of(Values.NULL),
            "list_test",
            "Invalid argument at position 2: null is not supported inside lists"),
        Arguments.of(
            Values.of(Values.of(1)),
            "set_test",
            "Invalid argument at position 2: Expected string type"),
        Arguments.of(
            Values.of(Values.NULL),
            "set_test",
            "Invalid argument at position 2: null is not supported inside sets"),
        Arguments.of(
            Values.of(Values.of("a"), Values.of("b")),
            "map_test",
            "Invalid argument at position 2: Expected integer type"),
        Arguments.of(
            Values.of(Values.of("a"), Values.of(1), Values.of("b")),
            "map_test",
            "Invalid argument at position 2: Expected an even number of elements"),
        Arguments.of(
            Values.of(Values.of("a"), Values.NULL),
            "map_test",
            "Invalid argument at position 2: null is not supported inside maps"),
        Arguments.of(
            Values.of(Values.NULL, Values.NULL),
            "map_test",
            "Invalid argument at position 2: null is not supported inside maps"),
        Arguments.of(
            Values.of(Values.of("a"), Values.of(1), Values.of(2)),
            "tuple_test",
            "Invalid argument at position 2: Expected UUID type"),
        Arguments.of(
            Values.of(Values.of("a"), Values.of(1), Values.of(UUID.randomUUID()), Values.of("b")),
            "tuple_test",
            "Invalid argument at position 2: Too many tuple fields. Expected 3, but received 4"));
  }
}
