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
package io.stargate.it.http.graphql.graphqlfirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class DeleteCustomConditionsTest extends GraphqlFirstTestBase {
  private static GraphqlFirstClient CLIENT;
  private static String KEYSPACE;
  private static CqlSession SESSION;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    CLIENT =
        new GraphqlFirstClient(
            cluster.seedAddress(), RestUtils.getAuthToken(cluster.seedAddress()));
    KEYSPACE = keyspaceId.asInternal();
    SESSION = session;
    CLIENT.deploySchema(
        KEYSPACE,
        "type Foo @cql_input {\n"
            + "  pk: Int! @cql_column(partitionKey: true)\n"
            + "  v: Int\n"
            + "}\n"
            + "type Query {\n"
            + "  foo(pk: Int!): Foo\n"
            + "}\n"
            + "type DeleteFooResponse @cql_payload {\n"
            + "  applied: Boolean\n"
            + "  foo: Foo\n"
            + "}\n"
            + "type Mutation {\n"
            + "  deleteFooGT(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if(field: \"v\", predicate: GT)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooLT(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if(field: \"v\", predicate: LT)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooLTE(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if(field: \"v\", predicate: LTE)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooGTE(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if(field: \"v\", predicate: GTE)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooEQ(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if(field: \"v\", predicate: EQ)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooNEQ(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if(field: \"v\", predicate: NEQ)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooIN(\n"
            + "    pk: Int\n"
            + "    vs: [Int] @cql_if(field: \"v\", predicate: IN)\n"
            + "    ): Boolean\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "  deleteFooWithResponsePayload(\n"
            + "    pk: Int\n"
            + "    v: Int @cql_if\n"
            + "    ): DeleteFooResponse\n"
            + "    @cql_delete(targetEntity: \"Foo\")\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    SESSION.execute("truncate table \"Foo\"");
  }

  private static void insert(int pk, int value) {
    SESSION.execute("INSERT INTO \"Foo\"(pk, v) VALUES (?, ?)", pk, value);
  }

  private static boolean exists(int pk) {
    ResultSet resultSet = SESSION.execute("SELECT * FROM \"Foo\" WHERE pk = ? ", pk);
    return resultSet.one() != null;
  }

  @Test
  @DisplayName("Should delete rows with cql_if EQ predicate")
  public void deleteWithCqlIfEqual() {
    // Given
    insert(1, 1000);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooEQ(pk: 1, v: 999) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooEQ")).isFalse();
    assertThat(exists(1)).isTrue();

    // when
    response = CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooEQ(pk: 1, v: 1000) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooEQ")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should delete rows with cql_if GT predicate")
  public void deleteWithGreaterThanPredicate() {
    // Given
    insert(1, 100);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooGT(pk: 1, v: 100) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooGT")).isFalse();
    assertThat(exists(1)).isTrue();

    // when
    response = CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooGT(pk: 1, v: 99) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooGT")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should delete rows with cql_if GTE predicate")
  public void deleteWithGreaterThanEqualPredicate() {
    // Given
    insert(1, 100);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooGTE(pk: 1, v: 100) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooGTE")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should delete rows with cql_if LT predicate")
  public void deleteWithLessThanPredicate() {
    // Given
    insert(1, 100);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooLT(pk: 1, v: 100) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooLT")).isFalse();
    assertThat(exists(1)).isTrue();

    // when
    response = CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooLT(pk: 1, v: 101) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooLT")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should delete rows with cql_if LTE predicate")
  public void deleteWithLessThanEqualPredicate() {
    // Given
    insert(1, 100);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooLTE(pk: 1, v: 100) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooLTE")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should delete rows with cql_if NEQ predicate")
  public void deleteWithCqlIfNotEqual() {
    // Given
    insert(1, 100);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooNEQ(pk: 1, v: 100) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooNEQ")).isFalse();
    assertThat(exists(1)).isTrue();

    // Deleting a non-existing row always returns true:
    // when
    response = CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooNEQ(pk: 1, v: 99) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooNEQ")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should delete rows with cql_if IN predicate")
  public void deleteWithCqlIfINisNotSupported() {
    // Given
    insert(1, 100);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooIN(pk: 1, vs: [99]) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooIN")).isFalse();
    assertThat(exists(1)).isTrue();

    // when
    response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { deleteFooIN(pk: 1, vs: [99, 100]) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.deleteFooIN")).isTrue();
    assertThat(exists(1)).isFalse();
  }

  @Test
  @DisplayName("Should return conflicting data in payload for failed LWT")
  public void failedDeleteWithEntityResponse() {
    // Given
    insert(1, 1);

    // When
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { d: deleteFooWithResponsePayload(pk: 1, v: 42) {\n"
                + "  applied, foo { v } }\n"
                + "}");

    // Then
    assertThat(JsonPath.<Boolean>read(response, "$.d.applied")).isFalse();
    assertThat(JsonPath.<Integer>read(response, "$.d.foo.v")).isEqualTo(1);
  }
}
