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
package io.stargate.it.http.graphql.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.UUID;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class FilesResourceTest extends GraphqlFirstTestBase {

  private static final String SCHEMA_CONTENTS =
      "type User { id: ID! name: String username: String } "
          + "type Query { getUser(id: ID!): User }";

  private static final UUID WRONG_SCHEMA_VERSION = UUID.randomUUID();
  private static UUID DEPLOYED_SCHEMA_VERSION;

  private static GraphqlFirstClient CLIENT;
  private static GraphqlFirstClient UNAUTHORIZED_CLIENT;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster, @TestKeyspace CqlIdentifier keyspaceId) {
    String host = cluster.seedAddress();
    CLIENT = new GraphqlFirstClient(host, RestUtils.getAuthToken(host));

    DEPLOYED_SCHEMA_VERSION = CLIENT.deploySchema(keyspaceId.asInternal(), SCHEMA_CONTENTS);
    assertThat(DEPLOYED_SCHEMA_VERSION).isNotEqualTo(WRONG_SCHEMA_VERSION);

    UNAUTHORIZED_CLIENT = new GraphqlFirstClient(host, "invalid auth token");
  }

  @Test
  @DisplayName("Should download `cql_directives.graphql` file")
  public void cqlDirectives() {
    assertThat(CLIENT.getCqlDirectivesFile()).contains("directive @cql_input");
  }

  @Test
  @DisplayName("Should download schema file")
  public void schemaFile(@TestKeyspace CqlIdentifier keyspaceId) {
    assertThat(CLIENT.getSchemaFile(keyspaceId.asInternal()))
        .contains("type User")
        .contains("type Query");
    assertThat(CLIENT.getSchemaFile(keyspaceId.asInternal(), DEPLOYED_SCHEMA_VERSION.toString()))
        .contains("type User")
        .contains("type Query");
  }

  @Test
  @DisplayName("Should return 404 if schema file coordinates do not exist")
  public void schemaFileNotFound(@TestKeyspace CqlIdentifier keyspaceId) {
    CLIENT.expectSchemaFileStatus("unknown_keyspace", Status.NOT_FOUND);
    CLIENT.expectSchemaFileStatus(
        keyspaceId.asInternal(), WRONG_SCHEMA_VERSION.toString(), Status.NOT_FOUND);
  }

  @Test
  @DisplayName("Should return 400 if schema file coordinates are malformed")
  public void schemaFileMalformed(@TestKeyspace CqlIdentifier keyspaceId) {
    CLIENT.expectSchemaFileStatus("malformed keyspace #!?", Status.BAD_REQUEST);
    CLIENT.expectSchemaFileStatus(keyspaceId.asInternal(), "NOT A UUID", Status.BAD_REQUEST);
  }

  @Test
  @DisplayName("Should return 401 if unauthorized")
  public void schemaFileUnauthorized(@TestKeyspace CqlIdentifier keyspaceId) {
    UNAUTHORIZED_CLIENT.expectSchemaFileStatus(
        keyspaceId.asInternal(), DEPLOYED_SCHEMA_VERSION.toString(), Status.UNAUTHORIZED);
  }
}
