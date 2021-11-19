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
package io.stargate.it.grpc;

import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.INT;
import static io.stargate.proto.QueryOuterClass.TypeSpec.Basic.TEXT;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlKeyspace;
import io.stargate.proto.Schema.CqlKeyspaceCreate;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.CqlTableCreate;
import io.stargate.proto.Schema.UserDefinedTypeCreate;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class CreateSchemaTest extends GrpcIntegrationTest {

  private static final TypeSpec INT_TYPE = TypeSpec.newBuilder().setBasic(INT).build();
  private static final TypeSpec TEXT_TYPE = TypeSpec.newBuilder().setBasic(TEXT).build();

  @Test
  public void shouldCreateKeyspace(CqlSession session) {
    // Given
    CqlIdentifier keyspace = CqlIdentifier.fromInternal("grpc_CreateSchemaTest");
    assertThat(session.getMetadata().getKeyspace(keyspace)).isEmpty();

    StargateBlockingStub stub = stubWithCallCredentials();

    CqlKeyspaceCreate createKeyspacePayload =
        CqlKeyspaceCreate.newBuilder()
            .setKeyspace(
                CqlKeyspace.newBuilder()
                    .setName(keyspace.asInternal())
                    .setSimpleReplication(
                        CqlKeyspace.SimpleReplication.newBuilder().setReplicationFactor(1).build())
                    .build())
            .setIfNotExists(true)
            .build();

    // When
    Response response = stub.createKeyspace(createKeyspacePayload);

    // Then
    assertThat(response.hasSchemaChange()).isTrue();
    SchemaChange schemaChange = response.getSchemaChange();
    assertThat(schemaChange.getChangeType()).isEqualTo(SchemaChange.Type.CREATED);
    assertThat(schemaChange.getTarget()).isEqualTo(SchemaChange.Target.KEYSPACE);
    assertThat(schemaChange.getKeyspace()).isEqualTo(keyspace.asInternal());
    assertThat(schemaChange.hasName()).isFalse();
    assertThat(schemaChange.getArgumentTypesCount()).isZero();

    assertThat(session.refreshSchema().getKeyspace(keyspace)).isPresent();

    // When
    response = stub.createKeyspace(createKeyspacePayload);

    // Then
    assertThat(response.hasSchemaChange()).isFalse();

    session.execute("DROP KEYSPACE " + keyspace.asCql(false));
  }

  @Test
  public void shouldCreateTable(CqlSession session, @TestKeyspace CqlIdentifier keyspace) {
    // Given
    StargateBlockingStub stub = stubWithCallCredentials();

    CqlTableCreate createTablePayload =
        CqlTableCreate.newBuilder()
            .setKeyspaceName(keyspace.asInternal())
            .setTable(
                CqlTable.newBuilder()
                    .setName("foo")
                    .addPartitionKeyColumns(
                        ColumnSpec.newBuilder().setName("pk1").setType(INT_TYPE).build())
                    .addClusteringKeyColumns(
                        ColumnSpec.newBuilder().setName("cc1").setType(INT_TYPE).build())
                    .addColumns(ColumnSpec.newBuilder().setName("v").setType(TEXT_TYPE).build())
                    .putClusteringOrders("cc1", Schema.ColumnOrderBy.DESC)
                    .build())
            .setIfNotExists(true)
            .build();

    // When
    Response response = stub.createTable(createTablePayload);

    // Then
    assertThat(response.hasSchemaChange()).isTrue();
    SchemaChange schemaChange = response.getSchemaChange();
    assertThat(schemaChange.getChangeType()).isEqualTo(SchemaChange.Type.CREATED);
    assertThat(schemaChange.getTarget()).isEqualTo(SchemaChange.Target.TABLE);
    assertThat(schemaChange.getKeyspace()).isEqualTo(keyspace.asInternal());
    assertThat(schemaChange.getName().getValue()).isEqualTo("foo");
    assertThat(schemaChange.getArgumentTypesCount()).isZero();

    assertThat(session.refreshSchema().getKeyspace(keyspace).flatMap(ks -> ks.getTable("foo")))
        .hasValueSatisfying(
            t ->
                assertThat(t.describe(false))
                    .contains(
                        "CREATE TABLE "
                            + keyspace.asCql(false)
                            + ".\"foo\" "
                            + "( \"pk1\" int, \"cc1\" int, \"v\" text, "
                            + "PRIMARY KEY (\"pk1\", \"cc1\") ) "
                            + "WITH CLUSTERING ORDER BY (\"cc1\" DESC)"));

    // When
    response = stub.createTable(createTablePayload);

    // Then
    assertThat(response.hasSchemaChange()).isFalse();
  }

  @Test
  public void shouldCreateUdt(CqlSession session, @TestKeyspace CqlIdentifier keyspace) {
    // Given
    StargateBlockingStub stub = stubWithCallCredentials();

    UserDefinedTypeCreate createUdtPayload =
        UserDefinedTypeCreate.newBuilder()
            .setKeyspaceName(keyspace.asInternal())
            .setUdt(
                Udt.newBuilder()
                    .setName("address")
                    .putFields("street", TEXT_TYPE)
                    .putFields("zip", INT_TYPE)
                    .putFields("city", TEXT_TYPE)
                    .build())
            .setIfNotExists(true)
            .build();

    // When
    Response response = stub.createUserDefinedType(createUdtPayload);

    // Then
    assertThat(response.hasSchemaChange()).isTrue();
    SchemaChange schemaChange = response.getSchemaChange();
    assertThat(schemaChange.getChangeType()).isEqualTo(SchemaChange.Type.CREATED);
    assertThat(schemaChange.getTarget()).isEqualTo(SchemaChange.Target.TYPE);
    assertThat(schemaChange.getKeyspace()).isEqualTo(keyspace.asInternal());
    assertThat(schemaChange.getName().getValue()).isEqualTo("address");
    assertThat(schemaChange.getArgumentTypesCount()).isZero();

    assertThat(
            session
                .refreshSchema()
                .getKeyspace(keyspace)
                .flatMap(ks -> ks.getUserDefinedType("address")))
        .hasValueSatisfying(
            t ->
                assertThat(t.describe(false))
                    .contains(
                        "CREATE TYPE "
                            + keyspace.asCql(false)
                            + ".\"address\" ( \"street\" text, \"zip\" int, \"city\" text )"));

    // When
    response = stub.createUserDefinedType(createUdtPayload);

    // Then
    assertThat(response.hasSchemaChange()).isFalse();
  }
}
