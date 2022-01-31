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
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TYPE address(street VARCHAR, number int);",
      "CREATE TABLE users_with_address(id int PRIMARY KEY, address address);"
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
    assertThat(response.getTablesCount() == 1).isTrue();
    assertThat(response.getTables(0).getName().equals("users_with_address")).isTrue();
    assertThat(response.getTables(0).getPartitionKeyColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getPartitionKeyColumns(0).getName().equals("id")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getPartitionKeyColumns(0)
                .getType()
                .getBasic()
                .equals(QueryOuterClass.TypeSpec.Basic.INT))
        .isTrue();
    assertThat(response.getTables(0).getColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getColumns(0).getName().equals("address")).isTrue();
    assertThat(response.getTables(0).getColumns(0).getType().getUdt().getName().equals("address"))
        .isTrue();

    assertThat(response.getTypesCount() == 1).isTrue();
    assertThat(response.getTypes(0).getName().equals("address")).isTrue();
    assertThat(
            response
                .getTypes(0)
                .getFieldsMap()
                .get("street")
                .getBasic()
                .equals(QueryOuterClass.TypeSpec.Basic.VARCHAR))
        .isTrue();
    assertThat(
            response
                .getTypes(0)
                .getFieldsMap()
                .get("number")
                .getBasic()
                .equals(QueryOuterClass.TypeSpec.Basic.INT))
        .isTrue();
  }
}
