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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TYPE udt (f1 int, f2 text, f3 uuid)",
      "CREATE TABLE IF NOT EXISTS udt_test (k text, v udt, PRIMARY KEY(k))",
      "CREATE TYPE address(street VARCHAR, number int);",
      "CREATE TABLE users_with_address(id int PRIMARY KEY, address address);"
    })
public class UdtTest extends GrpcIntegrationTest {

  @ParameterizedTest
  @MethodSource("udtValues")
  public void udts(Value udt, String tableName, @TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery(
                String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                queryParameters(keyspace),
                Values.of("a"),
                udt));
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
    assertThat(rs.getRows(0).getValues(1)).isEqualTo(udt);
  }

  public static Stream<Arguments> udtValues() {
    return Stream.of(
        Arguments.of(
            Values.udtOf(
                ImmutableMap.of(
                    "f1", Values.of(1), "f2", Values.of("a"), "f3", Values.of(UUID.randomUUID()))),
            "udt_test"));
  }

  @ParameterizedTest
  @MethodSource("emptyValues")
  public void emptyUdts(
      Value udt, Value expected, String tableName, @TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery(
                String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                queryParameters(keyspace),
                Values.of("b"),
                udt));
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
        Arguments.of(Values.udtOf(ImmutableMap.of()), Values.NULL, "udt_test"),
        Arguments.of(
            Values.udtOf(ImmutableMap.of("f1", Values.NULL, "f2", Values.NULL, "f3", Values.NULL)),
            Values.NULL,
            "udt_test"),
        Arguments.of(
            Values.udtOf(ImmutableMap.of("f1", Values.of(1))),
            Values.udtOf(ImmutableMap.of("f1", Values.of(1), "f2", Values.NULL, "f3", Values.NULL)),
            "udt_test"),
        Arguments.of(
            Values.udtOf(ImmutableMap.of("f2", Values.of("abc"))),
            Values.udtOf(
                ImmutableMap.of("f1", Values.NULL, "f2", Values.of("abc"), "f3", Values.NULL)),
            "udt_test"));
  }

  @ParameterizedTest
  @MethodSource("invalidValues")
  public void invalidUdts(
      Value udt, String tableName, String expectedMessage, @TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    assertThatThrownBy(
            () -> {
              Response response =
                  stub.executeQuery(
                      cqlQuery(
                          String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                          queryParameters(keyspace),
                          Values.of("b"),
                          udt));
              assertThat(response).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  @Test
  public void shouldInsertCorrectUdtFieldsRegardlessOfTheirOrder(
      @TestKeyspace CqlIdentifier keyspace) throws InvalidProtocolBufferException {
    // given
    StargateBlockingStub stub = stubWithCallCredentials();
    Value udtValue =
        Values.udtOf(ImmutableMap.of("street", Values.of("Long st"), "number", Values.of(123)));

    // when insert udt value
    Response response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO users_with_address (id, address) VALUES (?, ?)",
                queryParameters(keyspace),
                Values.of(1),
                udtValue));
    assertThat(response).isNotNull();

    // then
    response =
        stub.executeQuery(
            cqlQuery(
                "SELECT id, address FROM users_with_address WHERE id = 1",
                queryParameters(keyspace)));
    ResultSet resultSet = response.getResultSet();
    QueryOuterClass.UdtValue udtResult = resultSet.getRows(0).getValues(1).getUdt();
    assertThat(udtResult.getFieldsMap().get("street").getString()).isEqualTo("Long st");
    assertThat(udtResult.getFieldsMap().get("number").getInt()).isEqualTo(123);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        Arguments.of(
            Values.udtOf(ImmutableMap.of("f1", Values.of("string_instead_of_int"))),
            "udt_test",
            "Invalid argument at position 2: Expected integer type"));
  }
}
