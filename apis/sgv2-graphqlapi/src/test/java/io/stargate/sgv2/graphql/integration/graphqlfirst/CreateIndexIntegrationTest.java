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
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@TestProfile(IntegrationTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CreateIndexIntegrationTest extends GraphqlFirstIntegrationTest {

  private static final String SAI_INDEX_CLASS_NAME =
      "org.apache.cassandra.index.sai.StorageAttachedIndex";
  private static final CqlIdentifier USER_TABLE_ID = CqlIdentifier.fromInternal("User");

  @AfterEach
  public void cleanup() {
    deleteAllGraphqlSchemas();
    session.execute("DROP TABLE IF EXISTS \"User\"");
  }

  @Test
  @DisplayName("Should create regular index with default name")
  public void createRegularIndexDefaultName() {

    String schema =
        "type User {\n"
            + "  id: ID!\n"
            + "  name: String @cql_index\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    CqlIdentifier indexId = CqlIdentifier.fromInternal("User_name_idx");
    IndexMetadata index = deploySchemaAndGetIndex(keyspaceId, schema, indexId, session);

    assertThat(index.getTarget()).isEqualTo("name");
    assertDefaultOrSai(index, "name");
  }

  @Test
  @DisplayName("Should create regular index with custom name")
  public void createRegularIndexCustomName() {

    String schema =
        "type User {\n"
            + "  id: ID!\n"
            + "  name: String @cql_index(name: \"myIndex\")\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    CqlIdentifier indexId = CqlIdentifier.fromInternal("myIndex");
    IndexMetadata index = deploySchemaAndGetIndex(keyspaceId, schema, indexId, session);

    assertDefaultOrSai(index, "name");
  }

  /*
  TODO this test requires the ability to enable SASI indexes in cassandra.yaml.

  @Test
  @DisplayName("Should create custom index")
  public void createCustomIndex() {
    String schema =
        "type User {\n"
            + "  id: ID!\n"
            + "  name: String @cql_index(class: \"org.apache.cassandra.index.sasi.SASIIndex\""
            + "                          options: \"'mode': 'CONTAINS'\")\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    CqlIdentifier indexId = CqlIdentifier.fromInternal("User_name_idx");
    IndexMetadata index = deploySchemaAndGetIndex(keyspaceId, schema, indexId, session);

    assertThat(index.getTarget()).isEqualTo("name");
    assertThat(index.getKind()).isEqualTo(IndexKind.CUSTOM);
    assertThat(index.getClassName()).contains("org.apache.cassandra.index.sasi.SASIIndex");
    assertThat(index.getOptions())
        .hasSize(3)
        .containsEntry("target", "name")
        .containsEntry("class_name", "org.apache.cassandra.index.sasi.SASIIndex")
        .containsEntry("mode", "CONTAINS");
  }
  */

  @Test
  @DisplayName("Should create VALUES index on a list field")
  public void createListValuesIndex() {
    String schema =
        "type User {\n"
            + "  id: ID!\n"
            + "  names: [String] @cql_index(target: VALUES)\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    CqlIdentifier indexId = CqlIdentifier.fromInternal("User_names_idx");
    IndexMetadata index = deploySchemaAndGetIndex(keyspaceId, schema, indexId, session);

    assertDefaultOrSai(index, "values(names)");
  }

  @Test
  @DisplayName("Should fail to create VALUES index if field is not a list")
  public void createValuesIndexNotAList() {
    String schema =
        "type User {\n"
            + "  id: ID!\n"
            + "  name: String @cql_index(target: VALUES)\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    String error = client.getDeploySchemaError(keyspaceId.asInternal(), null, schema);
    assertThat(error).contains("The GraphQL schema that you provided contains CQL mapping errors");
  }

  @Test
  @DisplayName("Should create index on existing table")
  public void createIndexExistingTable() {
    UUID version1 =
        client.deploySchema(
            keyspaceId.asInternal(),
            "type User {\n"
                + "  id: ID!\n"
                + "  name: String\n"
                + "}\n"
                + "type Query { user(id: ID!): User }");

    String newSchema =
        "type User {\n"
            + "  id: ID!\n"
            + "  name: String @cql_index\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    CqlIdentifier indexId = CqlIdentifier.fromInternal("User_name_idx");
    IndexMetadata index =
        deploySchemaAndGetIndex(keyspaceId, version1, newSchema, indexId, session);

    assertDefaultOrSai(index, "name");
  }

  private IndexMetadata deploySchemaAndGetIndex(
      CqlIdentifier keyspaceId, String schema, CqlIdentifier indexId, CqlSession session) {
    return deploySchemaAndGetIndex(keyspaceId, null, schema, indexId, session);
  }

  private IndexMetadata deploySchemaAndGetIndex(
      CqlIdentifier keyspaceId,
      UUID expectedVersion,
      String schema,
      CqlIdentifier indexId,
      CqlSession session) {
    client.deploySchema(
        keyspaceId.asInternal(),
        expectedVersion == null ? null : expectedVersion.toString(),
        schema);
    return session
        .refreshSchema()
        .getKeyspace(keyspaceId)
        .flatMap(k -> k.getTable(USER_TABLE_ID))
        .flatMap(
            t -> {
              return t.getIndex(indexId);
            })
        .orElseThrow(AssertionError::new);
  }

  // Some storage backends default to SAI instead of regular secondary when no class name is
  // specified. Handle both so that the test can run everywhere.
  private void assertDefaultOrSai(IndexMetadata index, String expectedTarget) {
    assertThat(index.getTarget()).isEqualTo(expectedTarget);
    switch (index.getKind()) {
      case COMPOSITES:
        assertThat(index.getClassName()).isEmpty();
        assertThat(index.getOptions()).hasSize(1).containsEntry("target", expectedTarget);
        break;
      case CUSTOM:
        assertThat(index.getClassName()).contains(SAI_INDEX_CLASS_NAME);
        assertThat(index.getOptions())
            .hasSize(2)
            .containsEntry("target", expectedTarget)
            .containsEntry("class_name", SAI_INDEX_CLASS_NAME);
        break;
      default:
        fail("Unexpected index kind %s".formatted(index.getKind()));
    }
  }
}
