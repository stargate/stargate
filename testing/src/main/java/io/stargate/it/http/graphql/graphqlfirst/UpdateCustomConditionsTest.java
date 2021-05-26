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
            + "type Mutation {\n"
            + "  updateUser(user: UserInput!): Boolean @cql_update\n"
            + "  updateUserEQ(\n"
            + "    pk: Int\n"
            + "    username: String\n"
            + "    age: Int @cql_if(field: \"age\", predicate: EQ)\n"
            + "  ): Boolean\n"
            + "    @cql_update(targetEntity: \"User\")\n"
            + "  updateUserIfExists(user: UserInput!): Boolean @cql_update\n"
            + "  updateUserOnlyIfPresent(user: UserInput!): Boolean @cql_update(ifExists: true)\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    SESSION.execute("truncate table \"User\"");
  }

  @Test
  @DisplayName("Should conditional update user using EQ predicate")
  public void testConditionalUpdateUserUsingEQPredicate() {
    // given
    updateUser(1, 18, "Max");

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE, "mutation { updateUserEQ(pk: 1, age: 100, username: \"John\") }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserEQ")).isFalse();
    assertThat(getUserName(1)).isEqualTo("Max");

    // when
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE, "mutation { updateUserEQ(pk: 1, age: 18,  username: \"John\") }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserEQ")).isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should update if exists using method with naming convention")
  public void testIfExistsNamingConvention() {
    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserIfExists(user: { pk: 1, age: 100, username: \"John\" } ) }");

    // then should not update user, because it does not exists
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExists")).isFalse();
    assertThat(getUserRow(1)).isNull();

    // given inserted user
    updateUser(1, 100, "Max");

    // when update existing user
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserIfExists(user: { pk: 1, age: 18,  username: \"John\" } ) }");

    // then should update the user
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserIfExists")).isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  @Test
  @DisplayName("Should update if exists using the ifExists argument on the cql_update directive")
  public void testIfExistsUsingArgumentOnCqlUpdateDirective() {
    // when
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserOnlyIfPresent(user: { pk: 1, age: 100, username: \"John\" } ) }");

    // then should not update user, because it does not exists
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserOnlyIfPresent")).isFalse();
    assertThat(getUserRow(1)).isNull();

    // given inserted user
    updateUser(1, 100, "Max");

    // when update existing user
    response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            "mutation { updateUserOnlyIfPresent(user: { pk: 1, age: 18,  username: \"John\" } ) }");

    // then should update the user
    assertThat(JsonPath.<Boolean>read(response, "$.updateUserOnlyIfPresent")).isTrue();
    assertThat(getUserName(1)).isEqualTo("John");
  }

  private void updateUser(int pk1, int age, String username) {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            String.format(
                "mutation {\n"
                    + "  result: updateUser(user: {pk: %s, age: %s, username: \"%s\"})\n"
                    + "}",
                pk1, age, username));

    // Should have generated an id
    Boolean id = JsonPath.read(response, "$.result");
    assertThat(id).isTrue();
  }
}
