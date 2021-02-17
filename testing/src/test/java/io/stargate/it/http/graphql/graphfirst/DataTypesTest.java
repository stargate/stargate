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
package io.stargate.it.http.graphql.graphfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
public class DataTypesTest extends BaseOsgiIntegrationTest {

  private static final UUID ID = UUID.randomUUID();
  private static GraphqlFirstClient CLIENT;

  @BeforeAll
  public static void initClient(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new GraphqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @BeforeEach
  public void resetKeyspace(CqlSession session) {
    session.execute("DROP TABLE IF EXISTS graphql_schema");
    session.execute("DROP TABLE IF EXISTS \"Holder\"");
    session.execute("DROP TYPE IF EXISTS \"Location\"");
    session.execute("DROP TYPE IF EXISTS \"Address\"");
  }

  /**
   * @param typeName the type name, as it appears in a GraphQL schema.
   * @param cqlTypeHint the CQL type to use when creating the table (optional).
   * @param literalValue the test value as a GraphQL literal, ready to be concatenated to a query.
   * @param expectedResponseValue the value that we should get in the JSON response.
   */
  @ParameterizedTest
  @MethodSource("getValues")
  @DisplayName("Should write value with insert mutation and read it back with query")
  public void insertAndRetrieveValue(
      String typeName,
      String cqlTypeHint,
      String literalValue,
      Object expectedResponseValue,
      @TestKeyspace CqlIdentifier keyspaceId) {

    CLIENT.deploySchema(
        keyspaceId.asInternal(),
        String.format(
            "type Address @cql_entity(target: UDT) @cql_input { street: String } "
                // TODO freeze nested UDTs automatically and remove the type hint below
                + "type Location @cql_entity(target: UDT) @cql_input { id: String, address: Address @cql_column(typeHint: \"frozen<\\\"Address\\\">\") } "
                + "type Holder @cql_input { id: ID!, value: %s %s } "
                + "type Mutation { insertHolder(holder: HolderInput): Holder } "
                + "type Query { getHolder(id: ID!): Holder } ",
            typeName,
            (cqlTypeHint == null) ? "" : "@cql_column(typeHint: \"" + cqlTypeHint + "\")"));

    CLIENT.executeNamespaceQuery(
        keyspaceId.asInternal(),
        String.format(
            "mutation { insertHolder(holder: {id: \"%s\", value: %s}) { id } }", ID, literalValue));
    String selection;
    if ("Address".equals(typeName) || "[Address]".equals(typeName)) {
      selection = "value { street }";
    } else if ("Location".equals(typeName)) {
      selection = "value { id, address { street } } ";
    } else {
      selection = "value";
    }
    Object response =
        CLIENT.executeNamespaceQuery(
            keyspaceId.asInternal(),
            String.format("{ getHolder(id: \"%s\") { %s } }", ID, selection));
    assertThat(JsonPath.<Object>read(response, "$.getHolder.value"))
        .isEqualTo(expectedResponseValue);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Arguments[] getValues() {
    return new Arguments[] {

      // Built-in GraphQL scalars:
      arguments("Boolean", null, "true", true),
      arguments("Float", null, "3.14", 3.14),
      arguments("Int", null, "1", 1),
      arguments("String", null, "\"test\"", "test"),
      arguments("ID", null, String.format("\"%s\"", ID), ID.toString()),

      // Custom CQL scalars:
      arguments("Uuid", null, String.format("\"%s\"", ID), ID.toString()),
      arguments(
          "TimeUuid",
          null,
          "\"001dace0-6fe9-11eb-b738-558a72dd8356\"",
          "001dace0-6fe9-11eb-b738-558a72dd8356"),
      arguments("Inet", null, "\"127.0.0.1\"", "127.0.0.1"),
      arguments("Date", null, "\"2020-10-21\"", "2020-10-21"),
      arguments("Duration", null, "\"1h4m48s20ms\"", "1h4m48s20ms"),
      // For some scalars we allow multiple input types: for example BigInt (long) accepts either a
      // GraphQL Int or String, but always returns a string
      arguments("BigInt", null, "123456789", "123456789"),
      arguments("BigInt", null, "\"123456789\"", "123456789"),
      arguments("Ascii", null, "\"abc123\"", "abc123"),
      arguments("Decimal", null, "\"1e24\"", "1E+24"),
      arguments("Varint", null, "9223372036854775808", "9223372036854775808"),
      arguments("Varint", null, "\"9223372036854775808\"", "9223372036854775808"),
      arguments("Float32", null, "1.0", 1.0),
      arguments("Blob", null, "\"yv4=\"", "yv4="),
      arguments("SmallInt", null, "1", 1),
      arguments("TinyInt", null, "1", 1),
      arguments("Timestamp", null, "1296705900000", formatToSystemTimeZone(1296705900000L)),
      arguments(
          "Timestamp", null, "\"2011-02-03 04:05+0000\"", formatToSystemTimeZone(1296705900000L)),
      arguments("Time", null, "\"10:15:30.123456789\"", "10:15:30.123456789"),

      // Collections:
      arguments("[Int]", null, "[1,2,3]", ImmutableList.of(1, 2, 3)),
      // Switching to a CQL set doesn't change the in/out values, but the underlying CQL queries
      // will change so cover it
      arguments("[Int]", "set<int>", "[1,2,3]", ImmutableList.of(1, 2, 3)),
      arguments(
          "[[Int]]",
          null,
          "[[1,2,3],[4,5,6]]",
          ImmutableList.of(ImmutableList.of(1, 2, 3), ImmutableList.of(4, 5, 6))),

      // UDTs:
      arguments(
          "Address", null, "{ street: \"1 Main St\" }", ImmutableMap.of("street", "1 Main St")),
      arguments(
          "[Address]",
          null,
          "[ { street: \"1 Main St\" }, { street: \"2 Main St\" } ]",
          ImmutableList.of(
              ImmutableMap.of("street", "1 Main St"), ImmutableMap.of("street", "2 Main St"))),
      arguments(
          "Location",
          null,
          "{ id: \"Home\", address: { street: \"1 Main St\" } }",
          ImmutableMap.of("id", "Home", "address", ImmutableMap.of("street", "1 Main St"))),
    };
  }

  private static String formatToSystemTimeZone(long epochMillis) {
    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    parser.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));
    return parser.format(new Date(epochMillis));
  }
}
