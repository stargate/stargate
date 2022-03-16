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
package io.stargate.it.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlIndex;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TYPE address(street VARCHAR, number int, phone_numbers frozen<map<text,text>>);",
      "CREATE TABLE users_with_address(id int PRIMARY KEY, address address);",
      "CREATE TABLE test_index(k int PRIMARY KEY, v int, m map<int, int>)",
      "CREATE INDEX v_index ON test_index(v)",
      "CREATE INDEX m_keys_index ON test_index(KEYS(m))",
    })
public class SchemaTest extends BridgeIntegrationTest {

  @Test
  public void describeKeyspace(@TestKeyspace CqlIdentifier keyspace) {
    StargateBridgeBlockingStub stub = stubWithCallCredentials();

    Schema.CqlKeyspaceDescribe response =
        stub.describeKeyspace(
            Schema.DescribeKeyspaceQuery.newBuilder()
                .setKeyspaceName(keyspace.asInternal())
                .build());
    assertThat(response).isNotNull();
    assertThat(response.getTablesCount()).isEqualTo(2);

    CqlTable usersTable = findTable(response, "users_with_address");
    assertThat(usersTable.getPartitionKeyColumnsCount()).isEqualTo(1);
    ColumnSpec idColumn = usersTable.getPartitionKeyColumns(0);
    assertThat(idColumn.getName()).isEqualTo("id");
    assertThat(idColumn.getType().getBasic()).isEqualTo(TypeSpec.Basic.INT);
    assertThat(usersTable.getColumnsCount() == 1).isTrue();
    ColumnSpec addressColumn = usersTable.getColumns(0);
    assertThat(addressColumn.getName()).isEqualTo("address");
    assertThat(addressColumn.getType().getUdt().getName()).isEqualTo("address");

    assertThat(response.getTypesCount()).isEqualTo(1);
    TypeSpec.Udt addressType = response.getTypes(0);
    assertThat(addressType.getName()).isEqualTo("address");
    assertThat(addressType.getFieldsMap().get("street").getBasic())
        .isEqualTo(TypeSpec.Basic.VARCHAR);
    assertThat(addressType.getFieldsMap().get("number").getBasic()).isEqualTo(TypeSpec.Basic.INT);
    assertThat(addressType.getFieldsMap().get("phone_numbers").getMap())
        .satisfies(
            phonesType -> {
              assertThat(phonesType.getFrozen()).isTrue();
              assertThat(phonesType.getKey().getBasic()).isEqualTo(TypeSpec.Basic.VARCHAR);
              assertThat(phonesType.getValue().getBasic()).isEqualTo(TypeSpec.Basic.VARCHAR);
            });

    CqlTable indexTable = findTable(response, "test_index");
    assertThat(indexTable.getIndexesCount()).isEqualTo(2);

    CqlIndex vIndex = findIndex(indexTable, "v_index");
    assertThat(vIndex.getColumnName()).isEqualTo("v");
    assertThat(vIndex.getIndexingType()).isEqualTo(Schema.IndexingType.DEFAULT);
    assertThat(vIndex.getCustom()).isFalse();
    assertThat(vIndex.hasIndexingClass()).isFalse();
    assertThat(vIndex.getOptionsMap()).isEmpty();

    CqlIndex mKeysIndex = findIndex(indexTable, "m_keys_index");
    assertThat(mKeysIndex.getColumnName()).isEqualTo("m");
    assertThat(mKeysIndex.getIndexingType()).isEqualTo(Schema.IndexingType.KEYS);
    assertThat(mKeysIndex.getCustom()).isFalse();
    assertThat(mKeysIndex.hasIndexingClass()).isFalse();
    assertThat(mKeysIndex.getOptionsMap()).isEmpty();
  }

  private CqlTable findTable(Schema.CqlKeyspaceDescribe response, String tableName) {
    for (CqlTable table : response.getTablesList()) {
      if (tableName.equals(table.getName())) {
        return table;
      }
    }
    throw new AssertionError("Expected to find a table named " + tableName);
  }

  private CqlIndex findIndex(CqlTable table, String indexName) {
    for (CqlIndex index : table.getIndexesList()) {
      if (indexName.equals(index.getName())) {
        return index;
      }
    }
    throw new AssertionError("Expected to find an index named " + indexName);
  }
}
