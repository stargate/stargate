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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.Arrays;
import java.util.Collections;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UpdateIncrementalUpdatesIntegrationTest extends GraphqlFirstIntegrationTest {

  private Row getCounterRow(int k) {
    ResultSet resultSet = session.execute("SELECT * FROM \"Counters\" WHERE k = ? ", k);
    return resultSet.one();
  }

  private Row getListCounterRow(int k) {
    ResultSet resultSet = session.execute("SELECT * FROM \"ListCounters\" WHERE k = ? ", k);
    return resultSet.one();
  }

  @BeforeAll
  public void deploySchema() {
    // we need a dedicated table for counter because:
    // "Cannot mix counter and non counter columns in the same table"
    client.deploySchema(
        keyspaceId.asInternal(),
        "type Counters @cql_input {\n"
            + "  k: Int! @cql_column(partitionKey: true)\n"
            + "  c: Counter\n"
            + "  c2: Counter\n"
            + "}\n"
            + "type ListCounters @cql_input {\n"
            + "  k: Int! @cql_column(partitionKey: true)\n"
            + "  l: [Int]\n"
            + "}\n"
            + "type Query { counters(k: Int!): Counters }\n"
            + "type Mutation {\n"
            + " updateCountersIncrement(\n"
            + "    k: Int\n"
            + "    cInc: Int @cql_increment(field: \"c\")\n"
            + "  ): Boolean\n"
            + "@cql_update(targetEntity: \"Counters\")\n"
            + " updateTwoCountersIncrement(\n"
            + "    k: Int\n"
            + "    cInc: Int @cql_increment(field: \"c\")\n"
            + "    cInc2: Int @cql_increment(field: \"c2\")\n"
            + "  ): Boolean\n"
            + "@cql_update(targetEntity: \"Counters\")\n"
            + "  appendList(\n"
            + "    k: Int\n"
            + "    l: [Int] @cql_increment\n"
            + "  ): Boolean\n"
            + "@cql_update(targetEntity: \"ListCounters\")\n"
            + "  prependList(\n"
            + "    k: Int\n"
            + "    l: [Int] @cql_increment(prepend: true)\n"
            + "  ): Boolean\n"
            + "@cql_update(targetEntity: \"ListCounters\")\n"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    session.execute("truncate table \"Counters\"");
    session.execute("truncate table \"ListCounters\"");
  }

  @Test
  @DisplayName("Should update a counter field using increment operation")
  public void testUpdateCounterIncrement() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { updateCountersIncrement(k: 1, cInc: 2) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateCountersIncrement")).isTrue();
    assertThat(getCounterRow(1).get("c", TypeCodecs.COUNTER)).isEqualTo(2);

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { updateCountersIncrement(k: 1, cInc: 10) }");
    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateCountersIncrement")).isTrue();
    assertThat(getCounterRow(1).get("c", TypeCodecs.COUNTER)).isEqualTo(12);
  }

  @Test
  @DisplayName("Should update two counters field using increment operation")
  public void testUpdateTwoCountersIncrement() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "mutation { updateTwoCountersIncrement(k: 1, cInc: 2, cInc2: 4) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateTwoCountersIncrement")).isTrue();
    Row row = getCounterRow(1);
    assertThat(row.get("c", TypeCodecs.COUNTER)).isEqualTo(2);
    assertThat(getCounterRow(1).get("c2", TypeCodecs.COUNTER)).isEqualTo(4);

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(),
            "mutation { updateTwoCountersIncrement(k: 1, cInc: 10, cInc2: 12) }");
    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateTwoCountersIncrement")).isTrue();
    row = getCounterRow(1);
    assertThat(row.get("c", TypeCodecs.COUNTER)).isEqualTo(12);
    assertThat(row.get("c2", TypeCodecs.COUNTER)).isEqualTo(16);
  }

  @Test
  @DisplayName(
      "Should update(decrement) a counter field using increment operation with negative value")
  public void testUpdateCounterDecrementUsingNegativeValue() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { updateCountersIncrement(k: 1, cInc: 2) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateCountersIncrement")).isTrue();
    assertThat(getCounterRow(1).get("c", TypeCodecs.COUNTER)).isEqualTo(2);

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { updateCountersIncrement(k: 1, cInc: -1) }");
    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateCountersIncrement")).isTrue();
    assertThat(getCounterRow(1).get("c", TypeCodecs.COUNTER)).isEqualTo(1);
  }

  @Test
  @DisplayName("Should update a list field using append operation")
  public void testUpdateListAppend() {
    // when
    Object response =
        client.executeKeyspaceQuery(keyspaceId.asInternal(), "mutation { appendList(k: 1, l: 2) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.appendList")).isTrue();
    assertThat(getListCounterRow(1).getList("l", Integer.class))
        .isEqualTo(Collections.singletonList(2));

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { appendList(k: 1, l: 10) }");
    // then
    assertThat(JsonPath.<Boolean>read(response, "$.appendList")).isTrue();
    assertThat(getListCounterRow(1).getList("l", Integer.class)).isEqualTo(Arrays.asList(2, 10));
  }

  @Test
  @DisplayName("Should update a list field using prepend operation")
  public void testUpdateListPrepend() {
    // when
    Object response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { prependList(k: 1, l: 2) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.prependList")).isTrue();
    assertThat(getListCounterRow(1).getList("l", Integer.class))
        .isEqualTo(Collections.singletonList(2));

    // when
    response =
        client.executeKeyspaceQuery(
            keyspaceId.asInternal(), "mutation { prependList(k: 1, l: 10) }");
    // then
    assertThat(JsonPath.<Boolean>read(response, "$.prependList")).isTrue();
    assertThat(getListCounterRow(1).getList("l", Integer.class)).isEqualTo(Arrays.asList(10, 2));
  }
}
