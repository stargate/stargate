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
import static org.assertj.core.api.Assertions.assertThatCode;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class IntrospectionQueryTest extends BaseIntegrationTest {

  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @Test
  @DisplayName("The introspection query should succeed for a keyspace without any table")
  public void theIntrospectionQueryShouldSucceedForAKeyspaceWithoutAnyTable() {
    String keyspaceName = "library";

    Map<String, Object> response = createKeyspace(keyspaceName);
    assertThat(JsonPath.<Boolean>read(response, "$.createKeyspace")).isTrue();

    assertThatCode(() -> introspectionQuery(keyspaceName)).doesNotThrowAnyException();
  }

  private Map<String, Object> introspectionQuery(String keyspaceName) {
    return CLIENT.executeDmlQuery(
        CqlIdentifier.fromCql(keyspaceName), "{ __schema { types { name } } }");
  }

  private Map<String, Object> createKeyspace(String keyspaceName) {
    return CLIENT.executeDdlQuery(
        String.format(
            "mutation {\n"
                + "  createKeyspace(\n"
                + "    name:\"%s\", \n"
                + "    datacenters: { name:\"dc1\", replicas: 1 } \n"
                + "  )\n"
                + "}",
            keyspaceName));
  }
}
