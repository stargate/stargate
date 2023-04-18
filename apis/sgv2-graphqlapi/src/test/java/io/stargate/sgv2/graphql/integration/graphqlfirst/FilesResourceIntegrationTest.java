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
package io.stargate.sgv2.graphql.integration.graphqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import jakarta.ws.rs.core.Response.Status;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesResourceIntegrationTest extends GraphqlFirstIntegrationTest {

  private static final String SCHEMA_CONTENTS =
      "type User { id: ID! name: String username: String } "
          + "type Query { getUser(id: ID!): User }";

  private static final UUID WRONG_SCHEMA_VERSION = UUID.randomUUID();
  private static UUID DEPLOYED_SCHEMA_VERSION;

  @BeforeAll
  public void deploySchema() {
    DEPLOYED_SCHEMA_VERSION = client.deploySchema(keyspaceId.asInternal(), SCHEMA_CONTENTS);
    assertThat(DEPLOYED_SCHEMA_VERSION).isNotEqualTo(WRONG_SCHEMA_VERSION);
  }

  @Test
  @DisplayName("Should download `cql_directives.graphql` file")
  public void cqlDirectives() {
    assertThat(client.getCqlDirectivesFile()).contains("directive @cql_input");
  }

  @Test
  @DisplayName("Should download schema file")
  public void schemaFile() {
    assertThat(client.getSchemaFile(keyspaceId.asInternal()))
        .contains("type User")
        .contains("type Query");
    assertThat(client.getSchemaFile(keyspaceId.asInternal(), DEPLOYED_SCHEMA_VERSION.toString()))
        .contains("type User")
        .contains("type Query");
  }

  @Test
  @DisplayName("Should return 404 if schema file coordinates do not exist")
  public void schemaFileNotFound() {
    client.expectSchemaFileStatus("unknown_keyspace", Status.NOT_FOUND);
    client.expectSchemaFileStatus(
        keyspaceId.asInternal(), WRONG_SCHEMA_VERSION.toString(), Status.NOT_FOUND);
  }

  @Test
  @DisplayName("Should return 400 if schema file coordinates are malformed")
  public void schemaFileMalformed() {
    client.expectSchemaFileStatus("malformed keyspace #!?", Status.BAD_REQUEST);
    client.expectSchemaFileStatus(keyspaceId.asInternal(), "NOT A UUID", Status.BAD_REQUEST);
  }

  @Test
  @DisplayName("Should return 401 if unauthorized")
  @Disabled("Disabled until we find a way to pass an invalid token")
  public void schemaFileUnauthorized() {
    //    unauthorizedClient.expectSchemaFileStatus(
    //            keyspaceName, DEPLOYED_SCHEMA_VERSION.toString(), Status.UNAUTHORIZED);
  }
}
