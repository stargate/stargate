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
package io.stargate.bridge.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.bridge.Utils;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.Schema.QueryWithSchemaResponse;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class ExecuteQueryWithSchemaTest extends BaseBridgeServiceTest {

  private static final Keyspace KEYSPACE = ImmutableKeyspace.builder().name("ks").build();

  @Mock Schema schema;

  @BeforeEach
  public void mockKeyspace() {
    when(persistence.decorateKeyspaceName(any(), any()))
        .thenAnswer(
            i -> {
              String keyspaceName = i.getArgument(0);
              return mockDecorate(keyspaceName);
            });
    lenient().when(schema.keyspace(mockDecorate(KEYSPACE.name()))).thenReturn(KEYSPACE);
    when(persistence.schema()).thenReturn(schema);
  }

  @Test
  public void shouldReturnResponseWhenSchemaUpToDate() {
    QueryWithSchemaResponse withSchemaResponse =
        queryWithSchema(KEYSPACE.name(), KEYSPACE.schemaHashCode(), null);

    assertThat(withSchemaResponse.hasResponse()).isTrue();
    assertThat(withSchemaResponse.getResponse())
        .satisfies(
            response -> {
              assertThat(response.getResultSet().getRowsCount()).isEqualTo(1);
              assertThat(response.getResultSet().getRows(0).getValues(0).getString())
                  .isEqualTo("vValue");
            });
  }

  @Test
  public void shouldReturnNewSchemaWhenOutOfDate() {
    int wrongKeyspaceHash = KEYSPACE.schemaHashCode() + 1;
    QueryWithSchemaResponse withSchemaResponse =
        queryWithSchema(KEYSPACE.name(), wrongKeyspaceHash, null);

    assertThat(withSchemaResponse.hasNewKeyspace()).isTrue();
    assertThat(withSchemaResponse.getNewKeyspace().getHash().getValue())
        .isEqualTo(KEYSPACE.schemaHashCode());
  }

  @Test
  public void shouldReturnEmptyResponseWhenKeyspaceDeleted() {
    QueryWithSchemaResponse withSchemaResponse = queryWithSchema("nonExistingKs", 1, null);

    assertThat(withSchemaResponse.hasNoKeyspace()).isTrue();
  }

  @Test
  public void shouldHandleGrpcFailGracefully() {
    Exception fail = new StatusRuntimeException(Status.INVALID_ARGUMENT);
    assertThatThrownBy(() -> queryWithSchema(KEYSPACE.name(), KEYSPACE.schemaHashCode(), fail))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT")
        .extracting("status")
        .isEqualTo(Status.INVALID_ARGUMENT)
        .extracting("code")
        .isEqualTo(Status.INVALID_ARGUMENT.getCode());
  }

  @Test
  public void shouldHandleInternalNPEGracefully() {
    Exception fail = new NullPointerException("Missing stuff");
    assertThatThrownBy(() -> queryWithSchema(KEYSPACE.name(), KEYSPACE.schemaHashCode(), fail))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Missing stuff")
        .extracting("status")
        .extracting("code")
        .isEqualTo(Status.UNKNOWN.getCode());
  }

  private QueryWithSchemaResponse queryWithSchema(
      String keyspaceName, int keyspaceHash, Exception failWith) {
    final String query = "SELECT v FROM ks.tbl WHERE k = ?";

    Result.ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("v", Column.Type.Text));
    Result.Prepared prepared =
        new Result.Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("k", Column.Type.Text)),
            false,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    if (failWith != null) {
      when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
          .thenThrow(failWith);
    } else {
      when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
          .then(
              invocation -> {
                BoundStatement statement =
                    (BoundStatement) invocation.getArgument(0, Statement.class);
                assertStatement(prepared, statement, Values.of("kValue"));
                return CompletableFuture.completedFuture(
                    new Result.Rows(
                        Collections.singletonList(
                            Collections.singletonList(
                                TypeCodecs.TEXT.encode("vValue", ProtocolVersion.DEFAULT))),
                        resultMetadata));
              });
    }

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    return executeQueryWithSchema(
        makeBlockingStub(), keyspaceName, keyspaceHash, query, Values.of("kValue"));
  }

  private static String mockDecorate(String keyspaceName) {
    return "tenant_" + keyspaceName;
  }
}
