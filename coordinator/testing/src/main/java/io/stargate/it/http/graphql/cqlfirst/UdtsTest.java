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
      "CREATE TYPE IF NOT EXISTS \"B\"(i int)",
      "CREATE TYPE IF NOT EXISTS \"A\"(b frozen<\"B\">)",
      "CREATE TABLE IF NOT EXISTS \"Udts\"(a frozen<\"A\"> PRIMARY KEY, bs list<frozen<\"B\">>)",
      "CREATE TABLE IF NOT EXISTS \"Udts2\"(k int PRIMARY KEY, a \"A\")"
    })
public class UdtsTest extends BaseIntegrationTest {

  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @Test
  @DisplayName("Should insert and read back UDTs")
  public void udtsTest(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  insertUdts(value: {\n"
                + "    a: { b: {i:1} }\n"
                + "    bs: [ {i: 2}, {i: 3} ]"
                + "  }) {\n"
                + "        applied\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Boolean>read(response, "$.insertUdts.applied")).isTrue();

    response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "{\n"
                + "    Udts(value: { a: { b: {i:1} } }) {\n"
                + "        values {\n"
                + "            a { b { i } }\n"
                + "            bs { i }\n"
                + "        }\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Integer>read(response, "$.Udts.values[0].a.b.i")).isEqualTo(1);
    assertThat(JsonPath.<Integer>read(response, "$.Udts.values[0].bs[0].i")).isEqualTo(2);
    assertThat(JsonPath.<Integer>read(response, "$.Udts.values[0].bs[1].i")).isEqualTo(3);
  }

  @Test
  @DisplayName("Should insert null UDT")
  public void nullUdtTest(@TestKeyspace CqlIdentifier keyspaceId) {
    Map<String, Object> response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "mutation {\n"
                + "  insertUdts2(value: {\n"
                + "    k: 1\n"
                + "    a: null\n"
                + "  }) {\n"
                + "        applied\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Boolean>read(response, "$.insertUdts2.applied")).isTrue();

    response =
        CLIENT.executeDmlQuery(
            keyspaceId,
            "{\n"
                + "    Udts2(value: { k: 1 }) {\n"
                + "        values {\n"
                + "            a { b { i } }\n"
                + "        }\n"
                + "    }\n"
                + "}");
    assertThat(JsonPath.<Object>read(response, "$.Udts2.values[0].a")).isNull();
  }
}
