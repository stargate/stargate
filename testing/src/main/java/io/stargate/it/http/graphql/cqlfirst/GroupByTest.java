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
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.ApiServiceConnectionInfo;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.graphql.BaseGraphqlV2ApiTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS readings (\n"
          + "    id int, year int, month int, day int,\n"
          + "    value decimal,\n"
          + "    PRIMARY KEY (id, year, month, day)"
          + ") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)\n",
      "INSERT INTO readings (id, year, month, day, value) VALUES (1, 2021, 8, 30, 1.0)",
      "INSERT INTO readings (id, year, month, day, value) VALUES (1, 2021, 8, 31, 2.1)",
      "INSERT INTO readings (id, year, month, day, value) VALUES (1, 2021, 9, 1, 3.7)",
    })
public class GroupByTest extends BaseGraphqlV2ApiTest {
  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo stargateBackend, ApiServiceConnectionInfo stargateGraphqlApi) {
    CLIENT =
        new CqlFirstClient(
            stargateGraphqlApi.host(),
            stargateGraphqlApi.port(),
            RestUtils.getAuthToken(stargateBackend.seedAddress()));
  }

  @Test
  @DisplayName("Should execute query with groupBy")
  public void groupBy(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
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
