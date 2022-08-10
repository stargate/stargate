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

import com.jayway.jsonpath.JsonPath;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Basic;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.graphql.integration.util.CqlFirstIntegrationTest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.enterprise.context.control.ActivateRequestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
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
                keyspaceName, tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.createTable")).isTrue();

    Schema.CqlTable table = getTable(tableName).orElseThrow(AssertionError::new);

    assertThat(table.getPartitionKeyColumnsList())
        .extracting(ColumnSpec::getName)
        .containsExactly("id");
    assertThat(table.getPartitionKeyColumnsList())
        .extracting(c -> c.getType().getBasic())
        .containsExactly(Basic.UUID);

    assertThat(table.getColumnsList())
        .extracting(ColumnSpec::getName)
        .containsExactly("firstname", "lastname");
    assertThat(table.getColumnsList())
        .extracting(c -> c.getType().getBasic())
        .containsExactly(Basic.VARCHAR, Basic.VARCHAR);

    executeCql(String.format("DROP TABLE %s", tableName));
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
                keyspaceName, tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.createTable")).isTrue();

    Schema.CqlTable table = getTable(tableName).orElseThrow(AssertionError::new);

    assertThat(table.getPartitionKeyColumnsList())
        .extracting(ColumnSpec::getName)
        .containsExactly("pk1");
    assertThat(table.getClusteringKeyColumnsList())
        .extracting(ColumnSpec::getName)
        .containsExactly("ck1", "ck2");
    assertThat(table.getClusteringOrdersMap())
        .hasSize(2)
        .containsEntry("ck1", Schema.ColumnOrderBy.ASC)
        .containsEntry("ck2", Schema.ColumnOrderBy.DESC);

    executeCql(String.format("DROP TABLE %s", tableName));
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
            keyspaceName, tableName));

    Map<String, Object> response =
        client.executeDdlQuery(
            String.format(
                "mutation { dropTable(keyspaceName: \"%s\", tableName: \"%s\") }",
                keyspaceName, tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.dropTable")).isTrue();
    assertThat(getTable(tableName)).isEmpty();
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
            keyspaceName, tableName));

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
                keyspaceName, tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.alterTableAdd")).isTrue();
    Schema.CqlTable table = getTable(tableName).orElseThrow(AssertionError::new);
    assertThat(table.getColumnsList())
        .anySatisfy(
            c -> {
              assertThat(c.getName()).isEqualTo("name");
              assertThat(c.getType().getBasic()).isEqualTo(Basic.VARCHAR);
            });

    executeCql(String.format("DROP TABLE %s", tableName));
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
            keyspaceName, tableName));

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
                keyspaceName, tableName));

    assertThat(JsonPath.<Boolean>read(response, "$.alterTableDrop")).isTrue();
    Schema.CqlTable table = getTable(tableName).orElseThrow(AssertionError::new);
    assertThat(table.getColumnsList()).extracting(ColumnSpec::getName).doesNotContain("name");

    executeCql(String.format("DROP TABLE %s", tableName));
  }

  private Optional<Schema.CqlTable> getTable(String tableName) {
    Schema.CqlKeyspaceDescribe keyspace =
        bridge
            .describeKeyspace(
                Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build())
            .await()
            .indefinitely();
    return keyspace.getTablesList().stream().filter(t -> tableName.equals(t.getName())).findFirst();
  }
}
