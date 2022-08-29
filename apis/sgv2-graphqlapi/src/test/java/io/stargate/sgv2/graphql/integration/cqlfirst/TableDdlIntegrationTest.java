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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TableDdlIntegrationTest extends CqlFirstIntegrationTest {

  @Test
  public void getTables() {
    Map<String, Object> response =
        client.executeDdlQuery("{ keyspace(name: \"system\") { tables { name } } }");
    List<String> tableNames = JsonPath.read(response, "$.keyspace.tables[*].name");
    assertThat(tableNames).contains("local", "peers");
  }

  @Test
  public void getTable() {
    Map<String, Object> response =
        client.executeDdlQuery(
            "{\n"
                + "  keyspace(name: \"system\") {\n"
                + "    table(name: \"local\") {\n"
                + "      columns { name, type { basic } }\n"
                + "    }\n"
                + "  }\n"
                + "}");
    List<String> listenAddressTypes =
        JsonPath.read(response, "$.keyspace.table.columns[?(@.name=='listen_address')].type.basic");
    assertThat(listenAddressTypes).containsExactly("INET");
  }

  @Test
  public void createTable() {
    String tableName = "tbl_createtable_" + System.currentTimeMillis();

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation {\n"
                    + "  createTable(\n"
                    + "    keyspaceName: \"%s\"\n"
                    + "    tableName: \"%s\"\n"
                    + "    partitionKeys: [ {name: \"id\", type: { basic: UUID} } ]\n"
                    + "    values: [\n"
                    + "      {name: \"lastname\", type: { basic: TEXT} },\n"
                    + "      {name: \"firstname\", type: { basic: TEXT} }\n"
                    + "    ]\n"
                    + "  )\n"
                    + "}",
                keyspaceId.asInternal(), tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.createTable")).isTrue();

    TableMetadata table =
        session
            .refreshSchema()
            .getKeyspace(keyspaceId)
            .flatMap(ks -> ks.getTable(tableName))
            .orElseThrow(AssertionError::new);
    assertThat(table.getPartitionKey())
        .extracting(ColumnMetadata::getName)
        .containsExactly(CqlIdentifier.fromInternal("id"));
    assertThat(table.getColumn("id")).map(ColumnMetadata::getType).contains(DataTypes.UUID);
    assertThat(table.getColumn("lastname")).map(ColumnMetadata::getType).contains(DataTypes.TEXT);
    assertThat(table.getColumn("firstname")).map(ColumnMetadata::getType).contains(DataTypes.TEXT);

    session.execute(String.format("DROP TABLE %s", tableName));
  }

  @Test
  @DisplayName("Should create table with clustering keys")
  public void createTableWithClusteringKey() {
    String tableName = "tbl_createtable_with_ck_" + System.currentTimeMillis();

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation {\n"
                    + "  createTable(\n"
                    + "    keyspaceName: \"%s\"\n"
                    + "    tableName: \"%s\"\n"
                    + "    partitionKeys: [ {name: \"pk1\", type: { basic: INT} } ]\n"
                    + "    clusteringKeys: [\n"
                    + "      {name: \"ck1\", type: { basic: TIMEUUID} },\n"
                    + "      {name: \"ck2\", type: { basic: BIGINT}, order: \"DESC\" }\n"
                    + "    ]\n"
                    + "    values: [ {name: \"value1\", type: { basic: TEXT} } ]\n"
                    + "  )\n"
                    + "}",
                keyspaceId.asInternal(), tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.createTable")).isTrue();

    TableMetadata table =
        session
            .refreshSchema()
            .getKeyspace(keyspaceId)
            .flatMap(ks -> ks.getTable(tableName))
            .orElseThrow(AssertionError::new);
    assertThat(table.getPartitionKey())
        .extracting(ColumnMetadata::getName)
        .containsExactly(CqlIdentifier.fromInternal("pk1"));
    assertThat(table.getClusteringColumns().keySet())
        .extracting(ColumnMetadata::getName)
        .containsExactly(CqlIdentifier.fromInternal("ck1"), CqlIdentifier.fromInternal("ck2"));
    assertThat(table.getClusteringColumns().values())
        .containsExactly(ClusteringOrder.ASC, ClusteringOrder.DESC);

    session.execute(String.format("DROP TABLE %s", tableName));
  }

  @Test
  public void dropTable() {
    String tableName = "tbl_droptable_" + System.currentTimeMillis();

    client.executeDdlQuery(
        String.format(
            "mutation {\n"
                + "  createTable(\n"
                + "    keyspaceName: \"%s\"\n"
                + "    tableName: \"%s\"\n"
                + "    partitionKeys: [ {name: \"id\", type: { basic: UUID} } ]\n"
                + "  )\n"
                + "}",
            keyspaceId.asInternal(), tableName));

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation { dropTable(keyspaceName: \"%s\", tableName: \"%s\") }",
                keyspaceId.asInternal(), tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.dropTable")).isTrue();
    assertThat(
            session.refreshSchema().getKeyspace(keyspaceId).flatMap(ks -> ks.getTable(tableName)))
        .isEmpty();
  }

  @Test
  public void alterTableAdd() {
    String tableName = "tbl_altertableadd_" + System.currentTimeMillis();

    client.executeDdlQuery(
        String.format(
            "mutation {\n"
                + "  createTable(\n"
                + "    keyspaceName: \"%s\"\n"
                + "    tableName: \"%s\"\n"
                + "    partitionKeys: [ {name: \"id\", type: { basic: UUID} } ]\n"
                + "  )\n"
                + "}",
            keyspaceId.asInternal(), tableName));

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation {\n"
                    + "  alterTableAdd(\n"
                    + "    keyspaceName: \"%s\"\n"
                    + "    tableName: \"%s\"\n"
                    + "    toAdd: [ {name: \"name\", type: { basic: TEXT} } ]\n"
                    + "  )\n"
                    + "}",
                keyspaceId.asInternal(), tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.alterTableAdd")).isTrue();
    TableMetadata table =
        session
            .refreshSchema()
            .getKeyspace(keyspaceId)
            .flatMap(ks -> ks.getTable(tableName))
            .orElseThrow(AssertionError::new);
    assertThat(table.getColumn("name"))
        .hasValueSatisfying(c -> assertThat(c.getType()).isEqualTo(DataTypes.TEXT));

    session.execute(String.format("DROP TABLE %s", tableName));
  }

  @Test
  public void alterTableDrop() {
    String tableName = "tbl_altertabledrop_" + System.currentTimeMillis();

    client.executeDdlQuery(
        String.format(
            "mutation {\n"
                + "  createTable(\n"
                + "    keyspaceName: \"%s\"\n"
                + "    tableName: \"%s\"\n"
                + "    partitionKeys: [ {name: \"id\", type: { basic: UUID} } ]\n"
                + "    values: [ {name: \"name\", type: { basic: TEXT} } ]\n"
                + "  )\n"
                + "}",
            keyspaceId.asInternal(), tableName));

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation {\n"
                    + "  alterTableDrop(\n"
                    + "    keyspaceName: \"%s\"\n"
                    + "    tableName: \"%s\"\n"
                    + "    toDrop: [ \"name\" ]\n"
                    + "  )\n"
                    + "}",
                keyspaceId.asInternal(), tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.alterTableDrop")).isTrue();
    TableMetadata table =
        session
            .refreshSchema()
            .getKeyspace(keyspaceId)
            .flatMap(ks -> ks.getTable(tableName))
            .orElseThrow(AssertionError::new);
    assertThat(table.getColumn("name")).isEmpty();

    session.execute(String.format("DROP TABLE %s", tableName));
  }
}
