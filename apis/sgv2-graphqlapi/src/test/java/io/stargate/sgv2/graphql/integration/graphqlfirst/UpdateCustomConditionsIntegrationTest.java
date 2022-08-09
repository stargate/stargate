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

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.grpc.proto.Rows;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UpdateCustomConditionsIntegrationTest extends GraphqlFirstIntegrationTest {

  private String getUserName(int pk) {
    QueryOuterClass.ResultSet resultSet =
        executeCql("SELECT * FROM \"User\" WHERE pk = %d".formatted(pk)).getResultSet();
    return Rows.getString(resultSet.getRows(0), "username", resultSet.getColumnsList());
  }

  private void assertNoUserRow(int pk) {
    assertThat(
            executeCql("SELECT * FROM \"User\" WHERE pk = %d".formatted(pk))
                .getResultSet()
                .getRowsCount())
        .isEqualTo(0);
  }

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceName,
        "type User @cql_input {\n"
            + "  pk: Int! @cql_column(partitionKey: true)\n"
            + "  username: String"
            + "  age: Int\n"
            + "}\n"
            + "type Query { user(pk: Int!): User }\n"
            + "type UpdateUserResponse @cql_payload {\n"
            + "  applied: Boolean\n"
            + "  user: User!\n"
            + "}\n"
            + "type Mutation {\n"
            + "  updateUser(user: UserInput!): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserWherePkIn(\n"
            + "    pks: [Int] @cql_where(field: \"pk\", predicate: IN)\n"
            + "    username: String\n"
            + "  ): Boolean @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeEQ(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: EQ)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeNEQ(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: NEQ)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeGT(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: GT)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeGTE(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: GTE)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeLT(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: LT)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeLTE(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: LTE)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfAgeIN(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    ages: [Int] @cql_if(field: \"age\", predicate: IN)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfExists(user: UserInput!): UpdateUserResponse @cql_update\n"
            + "  updateUserIfExistsCustomPayload(user: UserInput!): UpdateUserResponse @cql_update(ifExists: true)\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    executeCql("truncate table \"User\"");
  }

  @Test
  @DisplayName("Should update users using IN predicate on PK")
  public void testUpdateUsingWhereIn() {
    // given
    updateUser(1, 18, "Max");
    updateUser(2, 21, "Sam");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName, "mutation { updateUserWherePkIn(pks: [1,2], username: \"Pat\") }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserWherePkIn")).isTrue();
    assertThat(getUserName(1)).isEqualTo("Pat");
    assertThat(getUserName(2)).isEqualTo("Pat");
  }

  @Test
  @DisplayName("Should conditionally update user using EQ predicate")
  public void testConditionalUpdateUserUsingEQPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeEQ(pk: 1, age: 100, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeEQ.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeEQ.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeEQ.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeEQ(pk: 1, age: 18, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeEQ.applied")).isTrue();
    // age contains the new value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeEQ.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeEQ.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should conditionally update user using NEQ predicate")
  public void testConditionalUpdateUserUsingNEQPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeNEQ(pk: 1, age: 18, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeNEQ.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeNEQ.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeNEQ.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeNEQ(pk: 1, age: 19, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeNEQ.applied")).isTrue();
    // age contains the new value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeNEQ.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeNEQ.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should conditionally update user using GT predicate")
  public void testConditionalUpdateUserUsingGTPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeGT(pk: 1, age: 100, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeGT.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeGT.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeGT.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeGT(pk: 1, age: 17, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeGT.applied")).isTrue();
    // age contains the new value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeGT.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeGT.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should conditionally update user using GTE predicate")
  public void testConditionalUpdateUserUsingGTEPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeGTE(pk: 1, age: 19, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeGTE.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeGTE.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeGTE.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeGTE(pk: 1, age: 18, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeGTE.applied")).isTrue();
    // age contains the new value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeGTE.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeGTE.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should conditionally update user using LT predicate")
  public void testConditionalUpdateUserUsingLTPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeLT(pk: 1, age: 18, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeLT.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeLT.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeLT.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeLT(pk: 1, age: 19, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeLT.applied")).isTrue();
    // age contains the new value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeLT.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeLT.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should conditionally update user using LTE predicate")
  public void testConditionalUpdateUserUsingLTEPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeLTE(pk: 1, age: 17, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeLTE.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeLTE.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeLTE.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeLTE(pk: 1, age: 18, username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeLTE.applied")).isTrue();
    // age contains the new value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeLTE.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeLTE.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should update if exists using method with naming convention")
  public void testIfExistsNamingConvention() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfExists(user: { pk: 1, age: 100, username: \"John\" } ) {applied} }");

    // then should not update user, because it does not exists
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExists.applied")).isFalse();
    assertNoUserRow(1);

    // given inserted user
    updateUser(1, 100, "Max");

    // when update existing user
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfExists(user: { pk: 1, age: 18,  username: \"John\" } ) {applied} }");

    // then should update the user
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExists.applied")).isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should update if exists returning with the custom payload return type")
  public void testIfExistsWithCustomPayloadReturnType() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfExistsCustomPayload(user: { pk: 1, age: 100, username: \"John\" } ) {applied} }");

    // then should not update user, because it does not exists
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExistsCustomPayload.applied"))
        .isFalse();
    assertNoUserRow(1);

    // given inserted user
    updateUser(1, 100, "Max");

    // when update existing user
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfExistsCustomPayload(user: { pk: 1, age: 18,  username: \"John\" } ) {applied} }");

    // then should update the user
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExistsCustomPayload.applied"))
        .isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should conditionally update user using IN predicate")
  public void testConditionalUpdateUserUsingINPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeIN(pk: 1, ages: [100], username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeIN.applied")).isFalse();
    // age contains the previous value (from DB)
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeIN.user.age")).isEqualTo(18);
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeIN.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceName,
            "mutation { updateUserIfAgeIN(pk: 1, ages: [18], username: \"John\") { \n"
                + "    applied"
                + "    user { age, username }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfAgeIN.applied")).isTrue();
    assertThat(JsonPath.<Integer>read(response, "$.updateUserIfAgeIN.user.age")).isNull();
    assertThat(JsonPath.<String>read(response, "$.updateUserIfAgeIN.user.username")).isNull();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  private void updateUser(int pk1, int age, String username) {
    Object response =
        client.executeKeyspaceQuery(
            keyspaceName,
            String.format(
                "mutation {\n"
                    + "  result: updateUser(user: {pk: %s, age: %s, username: \"%s\"}) \n "
                    + "{ applied }\n"
                    + "}",
                pk1, age, username));

    // Should have generated an id
    Boolean id = JsonPath.read(response, "$.result.applied");
    assertThat(id).isTrue();
  }
}
