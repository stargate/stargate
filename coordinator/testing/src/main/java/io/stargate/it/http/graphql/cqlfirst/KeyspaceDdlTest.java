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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.jayway.jsonpath.JsonPath;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class KeyspaceDdlTest extends BaseIntegrationTest {

  private static CqlFirstClient CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new CqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @Test
  public void getKeyspaces() {
    Map<String, Object> response = CLIENT.executeDdlQuery("{ keyspaces { name } }");
    List<String> keyspaceNames = JsonPath.read(response, "$.keyspaces[*].name");
    assertThat(keyspaceNames).contains("system");
  }

  @Test
  public void getKeyspace() {
    Map<String, Object> response =
        CLIENT.executeDdlQuery(
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
  public void createKeyspace(CqlSession session) {
    String name = "graphql_create_test";

    assertThat(session.execute(String.format("DROP KEYSPACE IF EXISTS %s", name)).wasApplied())
        .isTrue();

    Map<String, Object> response =
        CLIENT.executeDdlQuery(
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
