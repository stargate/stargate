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
public class UdtsIntegrationTest extends CqlFirstIntegrationTest {

  @BeforeAll
  public void createSchema() {
    session.execute("CREATE TYPE IF NOT EXISTS \"B\"(i int)");
    session.execute("CREATE TYPE IF NOT EXISTS \"A\"(b frozen<\"B\">)");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"Udts\"(a frozen<\"A\"> PRIMARY KEY, bs list<frozen<\"B\">>)");
    session.execute("CREATE TABLE IF NOT EXISTS \"Udts2\"(k int PRIMARY KEY, a \"A\")");
  }

  @Test
  @DisplayName("Should insert and read back UDTs")
  public void udtsTest() {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
  public void nullUdtTest() {
    Map<String, Object> response =
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
        client.executeDmlQuery(
            keyspaceId.asInternal(),
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
