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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
public class UpdateIncrementalUpdatesTest extends GraphqlFirstTestBase {

  private static CqlSession SESSION;
  private static GraphqlFirstClient CLIENT;
  private static String KEYSPACE;

  private static Row getCounterRow(int k) {
    ResultSet resultSet = SESSION.execute("SELECT * FROM \"Counter\" WHERE k = ? ", k);
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
        "type Counter @cql_input {\n"
            + "  k: Int! @cql_column(partitionKey: true)\n"
            + "  c: counter\n"
            + "}\n"
            + "type Query { counter(k: Int!): Counter }\n"
            + "type UpdateCounterResponse @cql_payload {\n"
            + "  applied: Boolean\n"
            + "  counter: Counter!\n"
            + "}\n"
            + "type Mutation {\n"
            + " updateCounter(\n"
            + "   counter: CounterInput! "
            + "  ): UpdateCounterResponse\n"
            + "@cql_update(targetEntity: \"Counter\")\n"
            + " updateCounterIncrement(\n"
            + "    k: Int\n"
            + "    cInc: Int @cql_increment(field: \"c\")\n"
            + "  ): Boolean\n"
            + "@cql_update(targetEntity: \"Counter\")\n"
            //            + "  \n"
            //            + "  appendList(\n"
            //            + "    k: Int\n"
            //            + "    l: [Int] @cql_increment\n"
            //            + "  )\n"
            //            + "  \n"
            //            + "  prependList(\n"
            //            + "    k: Int\n"
            //            + "    l: [Int] @cql_increment(prepend: true)\n"
            //            + "  )\n"
            //            + "  \n"
            //            + "  appendSet(\n"
            //            + "    k: Int\n"
            //            + "    s: [Int] @cql_increment\n"
            //            + "  )"
            + "}");
  }

  @BeforeEach
  public void cleanupData() {
    SESSION.execute("truncate table \"Counter\"");
  }

  @Test
  @DisplayName("Should update a counter field using increment operation")
  public void testUpdateCounterIncrement() {
    // given
    updateCounter(1, 10);

    // when
    Object response =
        CLIENT.executeKeyspaceQuery(KEYSPACE, "mutation { updateCounterIncrement(k: 1, cInc: 2) }");

    // then
    assertThat(JsonPath.<Boolean>read(response, "$.updateCounterIncrement")).isTrue();
    assertThat(getCounterRow(1).getInt("c")).isEqualTo(12);
  }

  private void updateCounter(int pk1, int c) {
    Object response =
        CLIENT.executeKeyspaceQuery(
            KEYSPACE,
            String.format(
                "mutation {\n"
                    + "  result: updateCounter(counter: {k: %s, c: %s}) \n "
                    + "{ applied }\n"
                    + "}",
                pk1, c));

    // Should have generated an id
    Boolean id = JsonPath.read(response, "$.result.applied");
    assertThat(id).isTrue();
  }
}
