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
package io.stargate.sgv2.graphql.integration.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CounterUpdateIntegrationTest extends CqlFirstIntegrationTest {

  @BeforeAll
  public void createSchema() {
    session.execute(
        "CREATE TABLE IF NOT EXISTS counters (\n"
            + "    k int PRIMARY KEY,\n"
            + "    c1 counter,\n"
            + "    c2 counter\n"
            + ")");
  }

  @Test
  @DisplayName("Should update counters with increments")
  public void updateCounters() {
    // Given
    String updateQuery = "mutation { updatecounters(value: {k: 1, c1: 1, c2: -2}) { applied } }";
    String selectQuery = "{ counters(value: { k: 1 }) { values { c1 c2 } } }";

    // When
    client.executeDmlQuery(keyspaceId.asInternal(), updateQuery);
    Map<String, Object> response = client.executeDmlQuery(keyspaceId.asInternal(), selectQuery);

    // Then
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c1")).isEqualTo("1");
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c2")).isEqualTo("-2");

    // When
    client.executeDmlQuery(keyspaceId.asInternal(), updateQuery);
    response = client.executeDmlQuery(keyspaceId.asInternal(), selectQuery);

    // Then
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c1")).isEqualTo("2");
    assertThat(JsonPath.<String>read(response, "$.counters.values[0].c2")).isEqualTo("-4");
  }
}
