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
import static org.assertj.core.api.Assertions.assertThatCode;

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntrospectionQueryIntegrationTest extends CqlFirstIntegrationTest {

  @Test
  @DisplayName("The introspection query should succeed for a keyspace without any table")
  public void theIntrospectionQueryShouldSucceedForAKeyspaceWithoutAnyTable() {
    String keyspaceName = "library";

    Map<String, Object> response = createKeyspace(keyspaceName);
    assertThat(JsonPath.<Boolean>read(response, "$.createKeyspace")).isTrue();

    assertThatCode(() -> introspectionQuery(keyspaceName)).doesNotThrowAnyException();
  }

  private Map<String, Object> introspectionQuery(String keyspaceName) {
    return client.executeDmlQuery(keyspaceName, "{ __schema { types { name } } }");
  }

  private Map<String, Object> createKeyspace(String keyspaceName) {
    return client.executeDdlQuery(
        String.format(
            "mutation {\n"
                + "  createKeyspace(\n"
                + "    name:\"%s\", \n"
                + "    datacenters: { name:\"datacenter1\", replicas: 1 } \n"
                + "  )\n"
                + "}",
            keyspaceName));
  }
}
