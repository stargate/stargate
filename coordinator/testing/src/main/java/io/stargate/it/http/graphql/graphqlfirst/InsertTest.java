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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class InsertTest extends GraphqlFirstTestBase {

  private static GraphqlFirstClient CLIENT;
  private static String KEYSPACE;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    CLIENT =
        new GraphqlFirstClient(
            cluster.seedAddress(), RestUtils.getAuthToken(cluster.seedAddress()));
    KEYSPACE = keyspaceId.asInternal();
    CLIENT.deploySchema(
        KEYSPACE,
        "type User @cql_input {\n"
            + "  id: ID!\n"
            + "  name: String\n"
            + "  username: String\n"
            + "}\n"
            + "type Query { user(id: ID!): User }\n"
            + "type InsertUserResponse @cql_payload {\n"
            + "  applied: Boolean!\n"
            + "  user: User!\n"
            + "}\n"
            + "type Mutation {\n"
            + "  insertUser(user: UserInput!): User\n"
            + "  insertUserReturnBoolean(user: UserInput!): Boolean\n"
            + "  persistUser(user: UserInput!): User @cql_insert\n"
            + "  insertUser2(user: UserInput!): InsertUserResponse\n"
            + "  insertUserIfNotExists(user: UserInput!): InsertUserResponse\n"
            + "  insertUser3(user: UserInput!): InsertUserResponse @cql_insert(ifNotExists: true)\n"
            + "}");
  }

  @Test
  @DisplayName("Should map simple insert")
  public void testSimpleInsert() {
    testSimpleInsert("insertUser");
  }

  @Test
  @DisplayName("Should map simple insert return boolean")
  public void testSimpleInsertReturnBoolean() {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation {\n"
                + "  result: insertUserReturnBoolean(user: {name: \"Ada Lovelace\", username: \"@ada\"})\n"
                + "}");

    // Should have generated an id
    Boolean applied = JsonPath.<Boolean>read(response, "$.result");
    assertThat(applied).isTrue();
  }

  @Test
  @DisplayName("Should map simple insert with unconventional name")
  public void testSimpleInsertUnconventional() {
    testSimpleInsert("persistUser");
  }

  private void testSimpleInsert(String mutationName) {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            String.format(
                "mutation {\n"
                    + "  result: %s(user: {name: \"Ada Lovelace\", username: \"@ada\"}) {\n"
                    + "    id, name, username\n"
                    + "  }\n"
                    + "}",
                mutationName));

    // Should have generated an id
    String id = JsonPath.read(response, "$.result.id");
    assertThatCode(() -> UUID.fromString(id)).doesNotThrowAnyException();

    String name = JsonPath.read(response, "$.result.name");
    assertThat(name).isEqualTo("Ada Lovelace");
    String username = JsonPath.read(response, "$.result.username");
    assertThat(username).isEqualTo("@ada");
  }

  @Test
  @DisplayName("Should map simple insert that returns a payload wrapper")
  public void testSimpleInsertReturningPayload() {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation {\n"
                + "  insertUser2(user: {name: \"Ada Lovelace\", username: \"@ada\"}) {\n"
                + "    applied"
                + "    user { id, name, username }\n"
                + "  }\n"
                + "}");

    // Apply will always be true for non-LWTs. Not very useful but we allow it.
    boolean applied = JsonPath.read(response, "$.insertUser2.applied");
    assertThat(applied).isTrue();

    // Same fields but they are nested inside the payload
    String id = JsonPath.read(response, "$.insertUser2.user.id");
    assertThatCode(() -> UUID.fromString(id)).doesNotThrowAnyException();
    String name = JsonPath.read(response, "$.insertUser2.user.name");
    assertThat(name).isEqualTo("Ada Lovelace");
    String username = JsonPath.read(response, "$.insertUser2.user.username");
    assertThat(username).isEqualTo("@ada");
  }

  @Test
  @DisplayName("Should map conditional insert")
  public void testConditionalInsert() {
    testConditionalInsert("insertUserIfNotExists");
  }

  @Test
  @DisplayName("Should map conditional insert with unconventional name")
  public void testConditionalInsertUnconventional() {
    testConditionalInsert("insertUser3");
  }

  private void testConditionalInsert(String mutationName) {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            String.format(
                "mutation {\n"
                    + "  result: %s(user: {name: \"Ada Lovelace\", username: \"@ada\"}) {\n"
                    + "    applied"
                    + "    user { id, name, username }\n"
                    + "  }\n"
                    + "}",
                mutationName));

    boolean applied = JsonPath.read(response, "$.result.applied");
    assertThat(applied).isTrue();
    String name = JsonPath.read(response, "$.result.user.name");
    assertThat(name).isEqualTo("Ada Lovelace");
    String username = JsonPath.read(response, "$.result.user.username");
    assertThat(username).isEqualTo("@ada");

    // Try to insert again with the same id
    UUID id = UUID.fromString(JsonPath.read(response, "$.result.user.id"));
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            String.format(
                "mutation {\n"
                    + "  result: %s(user: { id: \"%s\", name: \"Alan Turing\", username: \"@complete\"}) {\n"
                    + "    applied"
                    + "    user { id, name, username }\n"
                    + "  }\n"
                    + "}",
                mutationName, id));

    // The mutation should fail and return the previous user
    applied = JsonPath.read(response, "$.result.applied");
    assertThat(applied).isFalse();
    UUID echoedId = UUID.fromString(JsonPath.read(response, "$.result.user.id"));
    assertThat(echoedId).isEqualTo(id);
    name = JsonPath.read(response, "$.result.user.name");
    assertThat(name).isEqualTo("Ada Lovelace");
    username = JsonPath.read(response, "$.result.user.username");
    assertThat(username).isEqualTo("@ada");
  }
}
