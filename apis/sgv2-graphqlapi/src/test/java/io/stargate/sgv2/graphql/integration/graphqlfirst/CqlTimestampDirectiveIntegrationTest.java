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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.time.ZonedDateTime;
import java.util.Objects;
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
public class CqlTimestampDirectiveIntegrationTest extends GraphqlFirstIntegrationTest {

  private Long getUserWriteTimestamp(int k) {
    ResultSet resultSet = session.execute("SELECT writetime(v) FROM \"User\" WHERE k = ? ", k);
    return Objects.requireNonNull(resultSet.one()).getLong(0);
  }

  @BeforeAll
  public void deploySchema() {
    client.deploySchema(
        keyspaceId.asInternal(),
        "type User @cql_input {\n"
            + "  k: Int! @cql_column(partitionKey: true)\n"
            + "  v: Int\n"
            + "}\n"
            + "type Query { users(k: Int!): User }\n"
            + "type InsertUserResponse @cql_payload {\n"
            + "  applied: Boolean!\n"
            + "  user: User!\n"
            + "}\n"
            + "type Mutation {\n"
            + " updateWithWriteTimestamp(\n"
            + "    k: Int\n"
            + "    v: Int\n"
            + "    write_timestamp: BigInt @cql_timestamp\n"
            + "  ): Boolean\n"
            + "@cql_update(targetEntity: \"User\")\n"
            + "  insertWithWriteTimestamp(\n"
            + "    user: UserInput!\n"
            + "    write_timestamp: String @cql_timestamp\n"
            + "): InsertUserResponse @cql_insert\n"
            + "@cql_update(targetEntity: \"User\")\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    session.execute("truncate table \"User\"");
  }

  @Test
  @DisplayName(
      "Should update user with write timestamp using @cql_timestamp directive with a Long value")
  public void shouldUpdateUserWithWriteTimestampUsingCqlTimestampDirectiveLong() {
    // given
    Long writeTimestamp = 100_000L;

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            String.format(
                "mutation { updateWithWriteTimestamp(k: 1, v: 100, write_timestamp: \"%s\" ) }",
                writeTimestamp));

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateWithWriteTimestamp")).isTrue();
    assertThat(getUserWriteTimestamp(1)).isEqualTo(writeTimestamp);
  }

  @Test
  @DisplayName(
      "Should insert user with write timestamp using @cql_timestamp directive with a ZonedDateTime value")
  public void shouldInsertUserWithWriteTimestampUsingCqlTimestampDirectiveZonedDateTime() {
    // given
    String writeZonedDateTime = "2021-01-10T10:15:30+01:00[Europe/Paris]";

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            String.format(
                "mutation { insertWithWriteTimestamp(user: { k: 1, v: 100 }, write_timestamp: \"%s\" ) { \n"
                    + " applied, user { k, v } }\n"
                    + "}",
                writeZonedDateTime));

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.insertWithWriteTimestamp.applied")).isTrue();
    assertThat(getUserWriteTimestamp(1)).isEqualTo(toExpectedMicroseconds(writeZonedDateTime));
  }

  @Test
  @DisplayName(
      "Should update user with write timestamp using @cql_timestamp directive with a negative value")
  public void shouldUpdateUserWithWriteTimestampUsingCqlTimestampDirectiveNegativeValue() {
    // given
    Long writeTimestampNanos = -1L;

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            String.format(
                "mutation { updateWithWriteTimestamp(k: 1, v: 100, write_timestamp: \"%s\" ) }",
                writeTimestampNanos));

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateWithWriteTimestamp")).isTrue();
    assertThat(getUserWriteTimestamp(1)).isEqualTo(writeTimestampNanos);
  }

  @Test
  @DisplayName(
      "Should update user with write timestamp using @cql_timestamp directive with a Long.MAX_VALUE.")
  public void shouldUpdateUserWithWriteTimestampUsingCqlTimestampDirectiveLongMaxValue() {
    // given
    Long writeTimestampNanos = Long.MAX_VALUE;

    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            String.format(
                "mutation { updateWithWriteTimestamp(k: 1, v: 100, write_timestamp: \"%s\" ) }",
                writeTimestampNanos));

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateWithWriteTimestamp")).isTrue();
    assertThat(getUserWriteTimestamp(1)).isEqualTo(writeTimestampNanos);
  }

  @Test
  @DisplayName("Should fail insert user with write timestamp incorrect format")
  public void shouldFailToInsertUserWithWriteTimestampIncorrectFormat() {
    // given
    String writeZonedDateTime = "abc";

    // when
    String response =
        client.getKeyspaceError(
            keyspaceId.asInternal(),
            String.format(
                "mutation { insertWithWriteTimestamp(user: { k: 1, v: 100 }, write_timestamp: \"%s\" ) { \n"
                    + " applied, user { k, v } }\n"
                    + "}",
                writeZonedDateTime));

    // then
    assertThat(response)
        .isEqualTo(
            "Exception while fetching data (/insertWithWriteTimestamp) : Can't parse Timeout 'abc' (expected an ISO 8601 zoned date time string)");
  }

  private long toExpectedMicroseconds(String writeZonedDateTime) {
    ZonedDateTime dateTime = ZonedDateTime.parse(writeZonedDateTime);
    return dateTime.toEpochSecond() * 1_000_000 + dateTime.getNano() / 1000;
  }
}
