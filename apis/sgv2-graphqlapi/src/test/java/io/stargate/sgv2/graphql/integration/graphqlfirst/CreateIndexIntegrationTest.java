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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.GraphqlFirstIntegrationTest;
import java.util.UUID;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CreateIndexIntegrationTest extends GraphqlFirstIntegrationTest {

  private static final String SAI_INDEX_CLASS_NAME =
      "org.apache.cassandra.index.sai.StorageAttachedIndex";

  @AfterEach
  public void cleanup() {
    deleteAllGraphqlSchemas();
    executeCql("DROP TABLE IF EXISTS \"User\"");
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
    Schema.CqlIndex index = deploySchemaAndGetIndex(schema, "User_name_idx");

    assertThat(index.getColumnName()).isEqualTo("name");
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
    Schema.CqlIndex index = deploySchemaAndGetIndex(schema, "myIndex");

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
    Schema.CqlIndex index = deploySchemaAndGetIndex(schema, "User_name_idx");

    assertThat(index.getColumnName()).isEqualTo("name");
    assertThat(index.getCustom()).isTrue();
    assertThat(index.getIndexingClass().getValue())
        .contains("org.apache.cassandra.index.sasi.SASIIndex");
    assertThat(index.getOptionsMap())
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
    Schema.CqlIndex index = deploySchemaAndGetIndex(schema, "User_names_idx");

    assertDefaultOrSai(index, "names");
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
    String error = client.getDeploySchemaError(keyspaceName, null, schema);
    assertThat(error).contains("The GraphQL schema that you provided contains CQL mapping errors");
  }

  @Test
  @DisplayName("Should create index on existing table")
  public void createIndexExistingTable() {
    UUID version1 =
        client.deploySchema(
            keyspaceName,
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
    Schema.CqlIndex index = deploySchemaAndGetIndex(version1, newSchema, "User_name_idx");

    assertDefaultOrSai(index, "name");
  }

  private Schema.CqlIndex deploySchemaAndGetIndex(String schema, String indexName) {
    return deploySchemaAndGetIndex(null, schema, indexName);
  }

  private Schema.CqlIndex deploySchemaAndGetIndex(
      UUID expectedVersion, String schema, String indexName) {
    client.deploySchema(
        keyspaceName, expectedVersion == null ? null : expectedVersion.toString(), schema);
    Schema.CqlKeyspaceDescribe keyspace =
        bridge
            .describeKeyspace(
                Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build())
            .await()
            .indefinitely();
    Schema.CqlTable table =
        keyspace.getTablesList().stream()
            .filter(t -> t.getName().equals("User"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    return table.getIndexesList().stream()
        .filter(i -> i.getName().equals(indexName))
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  // Some storage backends default to SAI instead of regular secondary when no class name is
  // specified. Handle both so that the test can run everywhere.
  private void assertDefaultOrSai(Schema.CqlIndex index, String expectedTarget) {
    System.out.println(">>> Options: " + index.getOptionsMap());
    assertThat(index.getColumnName()).isEqualTo(expectedTarget);
    if (index.getCustom()) {
      assertThat(index.getIndexingClass().getValue()).contains(SAI_INDEX_CLASS_NAME);
      assertThat(index.getOptionsMap())
          .hasSize(1)
          .containsEntry("class_name", SAI_INDEX_CLASS_NAME);
    } else {
      assertThat(index.hasIndexingClass()).isFalse();
    }
  }
}
