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
package io.stargate.grpc.service;

import static io.stargate.db.schema.Column.Kind.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaOperationsTest extends BaseGrpcServiceTest {

  @Test
  @DisplayName(
      "Describe keyspace with a single table with columns using only simple types and no options")
  public void schemaDescribeSingleTableSimpleTypesNoOptions() {
    // Given
    StargateGrpc.StargateBlockingStub stub = makeBlockingStub();
    when(persistence.decorateKeyspaceName(any(String.class), any())).thenReturn("my_keyspace");

    io.stargate.db.schema.Schema schema =
        io.stargate.db.schema.Schema.build()
            .keyspace("my_keyspace")
            .table("my_table")
            .column("key", Column.Type.Text, PartitionKey)
            .column("leaf", Column.Type.Text, Clustering)
            .column("text_value", Column.Type.Text, Static)
            .column("dbl_value", Column.Type.Double)
            .column("bool_value", Column.Type.Boolean)
            .build();

    when(persistence.schema()).thenReturn(schema);
    startServer(persistence);

    // When
    Schema.CqlKeyspaceDescribe response =
        stub.describeKeyspace(
            Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName("my_keyspace").build());

    // Then
    assertThat(response.getCqlKeyspace().getName().equals("my_keyspace")).isTrue();
    assertThat(response.getTablesCount() == 1).isTrue();
    assertThat(response.getTables(0).getName().equals("my_table")).isTrue();
    assertThat(response.getTables(0).getPartitionKeyColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getPartitionKeyColumns(0).getName().equals("key")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getPartitionKeyColumns(0)
                .getType()
                .getBasic()
                .equals(TypeSpec.Basic.TEXT))
        .isTrue();
    assertThat(response.getTables(0).getClusteringKeyColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getClusteringKeyColumns(0).getName().equals("leaf")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getClusteringKeyColumns(0)
                .getType()
                .getBasic()
                .equals(TypeSpec.Basic.TEXT))
        .isTrue();
    assertThat(response.getTables(0).getStaticColumnsCount() == 1).isTrue();
    assertThat(response.getTables(0).getStaticColumns(0).getName().equals("text_value")).isTrue();
    assertThat(
            response
                .getTables(0)
                .getStaticColumns(0)
                .getType()
                .getBasic()
                .equals(TypeSpec.Basic.TEXT))
        .isTrue();
    assertThat(response.getTables(0).getColumnsCount() == 2).isTrue();
    Map<String, ColumnSpec> columnMap =
        response.getTables(0).getColumnsList().stream()
            .collect(Collectors.toMap(ColumnSpec::getName, Function.identity()));
    assertThat(columnMap.get("dbl_value").getType().getBasic().equals(TypeSpec.Basic.DOUBLE))
        .isTrue();
    assertThat(columnMap.get("bool_value").getType().getBasic().equals(TypeSpec.Basic.BOOLEAN))
        .isTrue();
  }
}
