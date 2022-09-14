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
package io.stargate.sgv2.graphql.integration.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BulkInsertIntegrationTest extends GraphqlFirstIntegrationTest {

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceId.asInternal(),
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
            + "  bulkInsertUsers(users: [UserInput!]): [User]\n"
            + "  bulkInsertUsersCustom(users: [UserInput!]): [InsertUserResponse]\n"
            + "  bulkInsertUsersBoolean(users: [UserInput!]): [Boolean]\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    session.execute("truncate table \"User\"");
  }

  @Test
  @DisplayName("Should map two inserts in one graphQL statement, returning list of entities")
  public void testBulkInsertReturnListOfEntities() {
    // given, when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "mutation {\n"
                + "bulkInsertUsers (users: [\n"
                + " { name: \"Ada Lovelace\", username: \"@ada\"},\n"
                + " { name: \"Alan Turing\", username: \"@alan\"}\n"
                + "]) \n"
                + "{ \n"
                + "id, name, username }\n"
                + "}");

    // then
    assertThatCode(() -> UUID.fromString(JsonPath.read(response, "$.bulkInsertUsers[0].id")))
        .doesNotThrowAnyException();
    assertThatCode(() -> UUID.fromString(JsonPath.read(response, "$.bulkInsertUsers[1].id")))
        .doesNotThrowAnyException();

    String name = JsonPath.read(response, "$.bulkInsertUsers[0].name");
    assertThat(name).isEqualTo("Ada Lovelace");
    String username = JsonPath.read(response, "$.bulkInsertUsers[0].username");
    assertThat(username).isEqualTo("@ada");
    name = JsonPath.read(response, "$.bulkInsertUsers[1].name");
    assertThat(name).isEqualTo("Alan Turing");
    username = JsonPath.read(response, "$.bulkInsertUsers[1].username");
    assertThat(username).isEqualTo("@alan");
  }

  @Test
  @DisplayName("Should map two inserts in one graphQL statement, returning list of custom payloads")
  public void testBulkInsertReturnListOfCustomPayloads() {
    // given, when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "mutation {\n"
                + "bulkInsertUsersCustom (users: [\n"
                + " { name: \"Ada Lovelace\", username: \"@ada\"},\n"
                + " { name: \"Alan Turing\", username: \"@alan\"}\n"
                + "]) { \n"
                + "    applied"
                + "    user { id, name, username }\n"
                + "   }\n"
                + "}");

    // then
    assertThatCode(
            () -> UUID.fromString(JsonPath.read(response, "$.bulkInsertUsersCustom[0].user.id")))
        .doesNotThrowAnyException();
    assertThatCode(
            () -> UUID.fromString(JsonPath.read(response, "$.bulkInsertUsersCustom[1].user.id")))
        .doesNotThrowAnyException();

    String name = JsonPath.read(response, "$.bulkInsertUsersCustom[0].user.name");
    assertThat(name).isEqualTo("Ada Lovelace");
    String username = JsonPath.read(response, "$.bulkInsertUsersCustom[0].user.username");
    assertThat(username).isEqualTo("@ada");
    name = JsonPath.read(response, "$.bulkInsertUsersCustom[1].user.name");
    assertThat(name).isEqualTo("Alan Turing");
    username = JsonPath.read(response, "$.bulkInsertUsersCustom[1].user.username");
    assertThat(username).isEqualTo("@alan");
  }

  @Test
  @DisplayName("Should map two inserts in one graphQL statement, returning list of booleans")
  public void testBulkInsertReturnListOfBooleans() {
    // given, when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "mutation {\n"
                + "bulkInsertUsersBoolean (users: [\n"
                + " { name: \"Ada Lovelace\", username: \"@ada\" },\n"
                + " { name: \"Alan Turing\", username: \"@alan\"}\n"
                + "]) \n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertUsersBoolean[0]")).isTrue();
    assertThat(JsonPath.<Boolean>read(response, "$.bulkInsertUsersBoolean[1]")).isTrue();
  }
}
