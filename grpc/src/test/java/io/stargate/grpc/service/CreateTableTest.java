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

import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.INT;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema.ColumnOrderBy;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.CqlTableCreate;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CreateTableTest extends BaseGrpcServiceTest {

  private static final ImmutableSchema SCHEMA =
      ImmutableSchema.builder()
          .addKeyspaces(ImmutableKeyspace.builder().name("test").build())
          .build();

  @BeforeEach
  public void setupSchema() {
    when(persistence.schema()).thenReturn(SCHEMA);
  }

  @Test
  @DisplayName("Should convert CreateTable operation to CQL query")
  public void createTable() {
    // Given
    CqlTableCreate createTablePayload =
        CqlTableCreate.newBuilder()
            .setKeyspaceName("test")
            .setTable(
                CqlTable.newBuilder()
                    .setName("foo")
                    .addPartitionKeyColumns(
                        ColumnSpec.newBuilder()
                            .setName("pk1")
                            .setType(TypeSpec.newBuilder().setBasic(INT).build())
                            .build())
                    .addClusteringKeyColumns(
                        ColumnSpec.newBuilder()
                            .setName("cc1")
                            .setType(TypeSpec.newBuilder().setBasic(INT).build())
                            .build())
                    .addColumns(
                        ColumnSpec.newBuilder()
                            .setName("v")
                            .setType(TypeSpec.newBuilder().setBasic(TEXT).build())
                            .build())
                    .putClusteringOrders("cc1", ColumnOrderBy.DESC)
                    .build())
            .build();
    AtomicReference<String> actualCql = new AtomicReference<>();

    when(persistence.decorateKeyspaceName(eq("test"), any())).thenReturn("test");
    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              SimpleStatement statement =
                  (SimpleStatement) invocation.getArgument(0, Statement.class);
              actualCql.set(statement.queryString());
              return CompletableFuture.completedFuture(
                  new Result.SchemaChange(
                      new Result.SchemaChangeMetadata(
                          "CREATED", "TABLE", "test", "foo", Collections.emptyList())));
            });
    when(connection.isInSchemaAgreement()).thenReturn(true);
    when(persistence.newConnection()).thenReturn(connection);
    startServer(persistence);

    // When
    Response response = makeBlockingStub().createTable(createTablePayload);

    // Then
    assertThat(response.hasSchemaChange()).isTrue();
    SchemaChange schemaChange = response.getSchemaChange();
    assertThat(schemaChange.getChangeType()).isEqualTo(SchemaChange.Type.CREATED);
    assertThat(schemaChange.getTarget()).isEqualTo(SchemaChange.Target.TABLE);
    assertThat(schemaChange.getKeyspace()).isEqualTo("test");
    assertThat(schemaChange.getName().getValue()).isEqualTo("foo");
    assertThat(schemaChange.getArgumentTypesCount()).isZero();

    assertThat(actualCql.get())
        .isEqualTo(
            "CREATE TABLE test.foo (pk1 int, cc1 int, v text, PRIMARY KEY ((pk1), cc1))"
                + " WITH CLUSTERING ORDER BY (cc1 DESC)");
  }
}
