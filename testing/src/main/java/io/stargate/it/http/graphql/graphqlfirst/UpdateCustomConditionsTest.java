package io.stargate.it.http.graphql.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class UpdateCustomConditionsTest extends GraphqlFirstTestBase {

  private static CqlSession SESSION;
  private static GraphqlFirstClient CLIENT;
  private static String KEYSPACE;

  private static String getUserName(int pk) {
    ResultSet resultSet = SESSION.execute("SELECT * FROM \"User\" WHERE pk = ? ", pk);
    return Objects.requireNonNull(resultSet.one()).getString("username");
  }

  private static Row getUserRow(int pk) {
    ResultSet resultSet = SESSION.execute("SELECT * FROM \"User\" WHERE pk = ? ", pk);
    return resultSet.one();
  }

  @BeforeAll
  public static void setup(
      StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    SESSION = session;
    CLIENT =
        new GraphqlFirstClient(
            cluster.seedAddress(), RestUtils.getAuthToken(cluster.seedAddress()));
    KEYSPACE = keyspaceId.asInternal();
    CLIENT.deploySchema(
        KEYSPACE,
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
            + "  updateUserEQCustomPayload(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: EQ)\n"
            + "  ): UpdateUserResponse @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfExists(user: UserInput!): UpdateUserResponse @cql_update\n"
            + "  updateUserIfExistsCustomPayload(user: UserInput!): UpdateUserResponse @cql_update(ifExists: true)\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    SESSION.execute("truncate table \"User\"");
  }

  @Test
  @DisplayName(
      "Should conditional update user using EQ predicate with a custom payload return type")
  public void testConditionalUpdateUserUsingPredicateWithCustomPayloadReturnType() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserEQCustomPayload(pk: 1, age: 100, username: \"John\") { \n"
                + "    applied"
                + "    user { age }\n"
                + "  }\n"
                + "}");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserEQCustomPayload.applied")).isFalse();
    // age contains the previous value
    assertThat(JsonPath.<Integer>read(response, "$.updateUserEQCustomPayload.user.age"))
        .isEqualTo(18);
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserEQCustomPayload(pk: 1, age: 18,  username: \"John\") {applied} }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserEQCustomPayload.applied")).isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should update if exists using method with naming convention")
  public void testIfExistsNamingConvention() {
    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserIfExists(user: { pk: 1, age: 100, username: \"John\" } ) {applied} }");

    // then should not update user, because it does not exists
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExists.applied")).isFalse();
    assertThat(getUserRow(1)).isNull();

    // given inserted user
    updateUser(1, 100, "Max");

    // when update existing user
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
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
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserIfExistsCustomPayload(user: { pk: 1, age: 100, username: \"John\" } ) {applied} }");

    // then should not update user, because it does not exists
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExistsCustomPayload.applied"))
        .isFalse();
    assertThat(getUserRow(1)).isNull();

    // given inserted user
    updateUser(1, 100, "Max");

    // when update existing user
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserIfExistsCustomPayload(user: { pk: 1, age: 18,  username: \"John\" } ) {applied} }");

    // then should update the user
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExistsCustomPayload.applied"))
        .isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  private void updateUser(int pk1, int age, String username) {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
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
