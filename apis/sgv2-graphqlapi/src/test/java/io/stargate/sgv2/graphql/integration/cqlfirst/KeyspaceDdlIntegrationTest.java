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

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KeyspaceDdlIntegrationTest extends CqlFirstIntegrationTest {

  @Test
  public void shouldGetKeyspaces() {
    Map<String, Object> response = client.executeDdlQuery("{ keyspaces { name } }");
    List<String> keyspaceNames = JsonPath.read(response, "$.keyspaces[*].name");
    assertThat(keyspaceNames).contains("system");
  }

  @Test
  public void shouldGetKeyspace() {
    Map<String, Object> response =
        client.executeDdlQuery(
            "{\n"
                + "  keyspace(name: \"system\") {\n"
                + "    name\n"
                + "    dcs { name, replicas }\n"
                + "    tables {\n"
                + "      name\n"
                + "      columns { name, type { basic } }\n"
                + "    }\n"
                + "  }\n"
                + "}");
    assertThat(JsonPath.<String>read(response, "$.keyspace.name")).isEqualTo("system");

    List<String> schemaVersionTypes =
        JsonPath.read(
            response,
            "$.keyspace.tables[?(@.name=='peers')]"
                + ".columns[?(@.name=='schema_version')].type.basic");
    assertThat(schemaVersionTypes).containsExactly("UUID");
  }

  @Test
  public void shouldCreateKeyspace() {
    String name = "graphql_create_test";

    session.execute(String.format("DROP KEYSPACE IF EXISTS %s", name));

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation { createKeyspace(name: \"%s\", ifNotExists: true, replicas: 1) }", name));

    assertThat(JsonPath.<Boolean>read(response, "$.createKeyspace")).isTrue();

    KeyspaceMetadata keyspace =
        session.refreshSchema().getKeyspace(name).orElseThrow(AssertionError::new);
    assertThat(keyspace.getReplication())
        .hasSize(2)
        .containsEntry("class", "org.apache.cassandra.locator.SimpleStrategy")
        .containsEntry("replication_factor", "1");
  }
}
