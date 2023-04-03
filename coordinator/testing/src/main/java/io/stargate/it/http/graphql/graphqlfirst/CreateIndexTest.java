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
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class CreateIndexTest extends GraphqlFirstTestBase {

  private static final String SAI_INDEX_CLASS_NAME =
      "org.apache.cassandra.index.sai.StorageAttachedIndex";

  private static GraphqlFirstClient CLIENT;
  private static final CqlIdentifier USER_TABLE_ID = CqlIdentifier.fromInternal("User");

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    String host = cluster.seedAddress();
    CLIENT = new GraphqlFirstClient(host, RestUtils.getAuthToken(host));
  }

  @AfterEach
  public void cleanup(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    deleteAllGraphqlSchemas(keyspaceId.asInternal(), session);
    session.execute("DROP TABLE IF EXISTS \"User\"");
  }

  @Test
  @DisplayName("Should create regular index with default name")
  public void createRegularIndexDefaultName(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {

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
  public void createRegularIndexCustomName(
      @TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {

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

  @Test
  @DisplayName("Should create custom index")
  public void createCustomIndex(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    // TODO remove this when we figure out how to enable SAI indexes in Cassandra 4
    assumeThat(isCassandra4())
        .as(
            "Disabled because it is currently not possible to enable SAI indexes "
                + "on a Cassandra 4 backend")
        .isFalse();

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

  @Test
  @DisplayName("Should create VALUES index on a list field")
  public void createListValuesIndex(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
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
  public void createValuesIndexNotAList(@TestKeyspace CqlIdentifier keyspaceId) {
    String schema =
        "type User {\n"
            + "  id: ID!\n"
            + "  name: String @cql_index(target: VALUES)\n"
            + "}\n"
            + "type Query { user(id: ID!): User }";
    String error = CLIENT.getDeploySchemaError(keyspaceId.asInternal(), null, schema);
    assertThat(error).contains("The GraphQL schema that you provided contains CQL mapping errors");
  }

  @Test
  @DisplayName("Should create index on existing table")
  public void createIndexExistingTable(@TestKeyspace CqlIdentifier keyspaceId, CqlSession session) {
    UUID version1 =
        CLIENT.deploySchema(
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
    CLIENT.deploySchema(
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
        fail("Unexpected index kind %s", index.getKind());
    }
  }
}
