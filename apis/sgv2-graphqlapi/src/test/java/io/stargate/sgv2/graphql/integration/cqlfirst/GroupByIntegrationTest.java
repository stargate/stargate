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
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GroupByIntegrationTest extends CqlFirstIntegrationTest {

  @BeforeAll
  public void createSchema() {
    session.execute(
        "CREATE TABLE IF NOT EXISTS readings (\n"
            + "    id int, year int, month int, day int,\n"
            + "    value decimal,\n"
            + "    PRIMARY KEY (id, year, month, day)"
            + ") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)\n");
    session.execute(
        "INSERT INTO readings (id, year, month, day, value) VALUES (1, 2021, 8, 30, 1.0)");
    session.execute(
        "INSERT INTO readings (id, year, month, day, value) VALUES (1, 2021, 8, 31, 2.1)");
    session.execute(
        "INSERT INTO readings (id, year, month, day, value) VALUES (1, 2021, 9, 1, 3.7)");
  }

  @Test
  @DisplayName("Should execute query with groupBy")
  public void groupBy() {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
            "{ readings(value: {id: 1}, groupBy: {month: true, year: true}) { "
                + "values { "
                + "  id"
                + "  year "
                + "  month "
                + "  totalValue: _decimal_function(name: \"sum\", args: [\"value\"]) } }"
                + "}");

    assertThat(JsonPath.<Integer>read(response, "$.readings.values[0].id")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.readings.values[0].year")).isEqualTo(2021);
    assertThat(JsonPath.<Integer>read(response, "$.readings.values[0].month")).isEqualTo(9);
    assertThat(JsonPath.<String>read(response, "$.readings.values[0].totalValue")).isEqualTo("3.7");

    assertThat(JsonPath.<Integer>read(response, "$.readings.values[1].id")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.readings.values[1].year")).isEqualTo(2021);
    assertThat(JsonPath.<Integer>read(response, "$.readings.values[1].month")).isEqualTo(8);
    assertThat(JsonPath.<String>read(response, "$.readings.values[1].totalValue")).isEqualTo("3.1");
  }
}
