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
public class DataTypesTest extends GraphqlFirstTestBase {

  private static final UUID ID = UUID.randomUUID();
  private static GraphqlFirstClient CLIENT;

  @BeforeAll
  public static void initClient(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new GraphqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @BeforeEach
  public void cleanupDb(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    deleteAllGraphqlSchemas(keyspaceId.asInternal(), session);
    session.execute("DROP TABLE IF EXISTS \"Holder\"");
    session.execute("DROP TYPE IF EXISTS \"Location\"");
    session.execute("DROP TYPE IF EXISTS \"Address\"");
  }

  /**
   * @param typeName the type name, as it appears in a GraphQL schema.
   * @param literalValue the test value as a GraphQL literal, ready to be concatenated to a query.
   * @param expectedResponseValue the value that we should get in the JSON response.
   * @param cqlTypeHint the CQL type to use when creating the table (optional).
   */
  @ParameterizedTest
  @MethodSource("getValues")
  @DisplayName("Should write value with insert mutation and read it back with query")
  public void insertAndRetrieveValue(
      String typeName,
      String literalValue,
      Object expectedResponseValue,
      String cqlTypeHint,
      @TestKeyspace CqlIdentifier keyspaceId) {

    CLIENT.deploySchema(
        keyspaceId.asInternal(),
        String.format(
            "type Address @cql_entity(target: UDT) @cql_input { street: String } "
                + "type Location @cql_entity(target: UDT) @cql_input { id: String, address: Address } "
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
      arguments("Boolean", "true", true, null),
      arguments("Float", "3.14", 3.14, null),
      arguments("Int", "1", 1, null),
      arguments("String", "\"test\"", "test", null),
      arguments("ID", String.format("\"%s\"", ID), ID.toString(), null),

      // Custom CQL scalars:
      arguments("Uuid", String.format("\"%s\"", ID), ID.toString(), null),
      arguments(
          "TimeUuid",
          "\"001dace0-6fe9-11eb-b738-558a72dd8356\"",
          "001dace0-6fe9-11eb-b738-558a72dd8356",
          null),
      arguments("Inet", "\"127.0.0.1\"", "127.0.0.1", null),
      arguments("Date", "\"2020-10-21\"", "2020-10-21", null),
      arguments("Duration", "\"1h4m48s20ms\"", "1h4m48s20ms", null),
      // For some scalars we allow multiple input types: for example BigInt (long) accepts either a
      // GraphQL Int or String, but always returns a string
      arguments("BigInt", "123456789", "123456789", null),
      arguments("BigInt", "\"123456789\"", "123456789", null),
      arguments("Ascii", "\"abc123\"", "abc123", null),
      arguments("Decimal", "\"1e24\"", "1E+24", null),
      arguments("Varint", "9223372036854775808", "9223372036854775808", null),
      arguments("Varint", "\"9223372036854775808\"", "9223372036854775808", null),
      arguments("Float32", "1.0", 1.0, null),
      arguments("Blob", "\"yv4=\"", "yv4=", null),
      arguments("SmallInt", "1", 1, null),
      arguments("TinyInt", "1", 1, null),
      arguments("Timestamp", "1296705900000", formatToSystemTimeZone(1296705900000L), null),
      arguments(
          "Timestamp", "\"2011-02-03 04:05+0000\"", formatToSystemTimeZone(1296705900000L), null),
      arguments("Time", "\"10:15:30.123456789\"", "10:15:30.123456789", null),

      // Collections:
      arguments("[Int]", "[1,2,3]", ImmutableList.of(1, 2, 3), null),
      // Switching to a CQL set doesn't change the in/out values, but the underlying CQL queries
      // will change so cover it
      arguments("[Int]", "[1,2,3]", ImmutableList.of(1, 2, 3), "set<int>"),
      arguments(
          "[[Int]]",
          "[[1,2,3],[4,5,6]]",
          ImmutableList.of(ImmutableList.of(1, 2, 3), ImmutableList.of(4, 5, 6)),
          null),

      // UDTs:
      arguments(
          "Address", "{ street: \"1 Main St\" }", ImmutableMap.of("street", "1 Main St"), null),
      arguments(
          "[Address]",
          "[ { street: \"1 Main St\" }, { street: \"2 Main St\" } ]",
          ImmutableList.of(
              ImmutableMap.of("street", "1 Main St"), ImmutableMap.of("street", "2 Main St")),
          null),
      arguments(
          "Location",
          "{ id: \"Home\", address: { street: \"1 Main St\" } }",
          ImmutableMap.of("id", "Home", "address", ImmutableMap.of("street", "1 Main St")),
          null),
    };
  }

  private static String formatToSystemTimeZone(long epochMillis) {
    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    parser.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));
    return parser.format(new Date(epochMillis));
  }
}
