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
package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS counters (\n"
          + "    k int PRIMARY KEY,\n"
          + "    c1 counter,\n"
          + "    c2 counter\n"
          + ")",
    })
public class CounterUpdateTest extends BaseIntegrationTest {

  private static CqlFirstClient CLIENT;
  private static CqlIdentifier KEYSPACE_ID;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
    KEYSPACE_ID = keyspaceId;
  }

  @Test
  @DisplayName("Should update counters with increments")
  public void updateCounters() {
    // Given
    String updateQuery = "mutation { updatecounters(value: {k: 1, c1: 1, c2: -2}) { applied } }";
    String selectQuery = "{ counters(value: { k: 1 }) { values { c1 c2 } } }";

    // When
    CLIENT.executeDmlQuery(KEYSPACE_ID, updateQuery);
    Map<String, Object> response = CLIENT.executeDmlQuery(KEYSPACE_ID, selectQuery);

    // Then
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c1")).isEqualTo("1");
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c2")).isEqualTo("-2");

    // When
    CLIENT.executeDmlQuery(KEYSPACE_ID, updateQuery);
    response = CLIENT.executeDmlQuery(KEYSPACE_ID, selectQuery);

    // Then
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c1")).isEqualTo("2");
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c2")).isEqualTo("-4");
  }
}
