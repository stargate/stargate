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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS \"Tuples\"(\n"
          + "    id uuid PRIMARY KEY,\n"
          + "    tuple1 tuple<bigint>,\n"
          + "    tuple2 tuple<float, float>,\n"
          + "    tuple3 tuple<timeuuid, int, boolean>\n"
          + ")",
      "CREATE TABLE IF NOT EXISTS \"TuplesPk\"(id tuple<int, int> PRIMARY KEY)"
    })
public class TuplesTest extends BaseGraphqlV2ApiTest {

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
  public void shouldInsertAndUpdateTuples(@TestKeyspace CqlIdentifier keyspaceId) {
    // When inserting a new row:
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  insertTuples: insertTuplx65_s(value: {\n"
                + "    id: \"792d0a56-bb46-4bc2-bc41-5f4a94a83da9\"\n"
                + "    tuple1: { item0: 1 }\n"
                + "    tuple2: { item0: 1.3, item1: -90 }\n"
                + "    tuple3: { item0: \"fe8a70f0-a947-11eb-8a78-15a2af3b9d20\"\n"
                + "              item1: 2\n"
                + "              item2: true }\n"
                + "  }) {\n"
                + "        applied\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Boolean>read(response, "$.insertTuples.applied")).isTrue();

    // Then the data can be read back:
    String getQuery =
        "{\n"
            + "  Tuples: Tuplx65_s(value: { id: \"792d0a56-bb46-4bc2-bc41-5f4a94a83da9\"}) {\n"
            + "    values {\n"
            + "      tuple1 { item0 }\n"
            + "      tuple2 { item0, item1 }\n"
            + "      tuple3 { item0, item1, item2 }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    response = CLIENT.executeDmlQuery(keyspaceId, getQuery);
    assertThat(JsonPath.<String>read(response, "$.Tuples.values[0].tuple1.item0")).isEqualTo("1");

    assertThat(JsonPath.<Double>read(response, "$.Tuples.values[0].tuple2.item0")).isEqualTo(1.3);
    assertThat(JsonPath.<Double>read(response, "$.Tuples.values[0].tuple2.item1")).isEqualTo(-90);

    assertThat(JsonPath.<String>read(response, "$.Tuples.values[0].tuple3.item0"))
        .isEqualTo("fe8a70f0-a947-11eb-8a78-15a2af3b9d20");
    assertThat(JsonPath.<Integer>read(response, "$.Tuples.values[0].tuple3.item1")).isEqualTo(2);
    assertThat(JsonPath.<Boolean>read(response, "$.Tuples.values[0].tuple3.item2")).isTrue();

    // When updating the row:
    response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  updateTuples: updateTuplx65_s(\n"
                + "    value: {\n"
                + "      id: \"792d0a56-bb46-4bc2-bc41-5f4a94a83da9\""
                + "    tuple1: { item0: -1 }\n"
                + "    tuple2: { item0: 0, item1: 431270.88 }\n"
                + "    tuple3: { item0: \"fe8a70f0-a947-11eb-8a78-15a2af3b9d20\"\n"
                + "              item1: 3\n"
                + "              item2: false }\n"
                + "  }) { applied } }");
    assertThat(JsonPath.<Boolean>read(response, "$.updateTuples.applied")).isTrue();

    // Then the changes are reflected:
    response = CLIENT.executeDmlQuery(keyspaceId, getQuery);
    assertThat(JsonPath.<String>read(response, "$.Tuples.values[0].tuple1.item0")).isEqualTo("-1");

    assertThat(JsonPath.<Double>read(response, "$.Tuples.values[0].tuple2.item0")).isEqualTo(0);
    assertThat(JsonPath.<Double>read(response, "$.Tuples.values[0].tuple2.item1"))
        .isEqualTo(431270.88);

    assertThat(JsonPath.<String>read(response, "$.Tuples.values[0].tuple3.item0"))
        .isEqualTo("fe8a70f0-a947-11eb-8a78-15a2af3b9d20");
    assertThat(JsonPath.<Integer>read(response, "$.Tuples.values[0].tuple3.item1")).isEqualTo(3);
    assertThat(JsonPath.<Boolean>read(response, "$.Tuples.values[0].tuple3.item2")).isFalse();
  }

  @Test
  public void shouldSupportTuplesAsPartitionKey(@TestKeyspace CqlIdentifier keyspaceId) {
    // When inserting a new row:
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  insertTuplesPk: insertTuplx65_sPk(value: {\n"
                + "    id: { item0: 0, item1: 1}\n"
                + "  }) {\n"
                + "        applied\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Boolean>read(response, "$.insertTuplesPk.applied")).isTrue();

    // Then the data can be read back:
    String getQuery =
        "{\n"
            + "  TuplesPk: Tuplx65_sPk(value: { id: { item0: 0, item1: 1} }) {\n"
            + "    values {\n"
            + "      id { item0, item1 }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    response = CLIENT.executeDmlQuery(keyspaceId, getQuery);
    assertThat(JsonPath.<Integer>read(response, "$.TuplesPk.values[0].id.item0")).isEqualTo(0);
    assertThat(JsonPath.<Integer>read(response, "$.TuplesPk.values[0].id.item1")).isEqualTo(1);
  }

  @Test
  public void shouldInsertNullTuple(@TestKeyspace CqlIdentifier keyspaceId) {
    // When inserting a new row:
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  insertTuples: insertTuplx65_s(value: {\n"
                + "    id: \"792d0a56-bb46-4bc2-bc41-5f4a94a83da9\"\n"
                + "    tuple1: null\n"
                + "  }) {\n"
                + "        applied\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Boolean>read(response, "$.insertTuples.applied")).isTrue();

    // Then the data can be read back:
    String getQuery =
        "{\n"
            + "  Tuples: Tuplx65_s(value: { id: \"792d0a56-bb46-4bc2-bc41-5f4a94a83da9\"}) {\n"
            + "    values {\n"
            + "      tuple1 { item0 }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    response = CLIENT.executeDmlQuery(keyspaceId, getQuery);
    assertThat(JsonPath.<Object>read(response, "$.Tuples.values[0].tuple1")).isNull();
  }
}
