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
import static org.mockito.Mockito.when;

import io.grpc.StatusRuntimeException;
import io.stargate.bridge.Utils;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaAgreementTest extends BaseBridgeServiceTest {

  @Test
  @DisplayName("Should succeed if schema agreement reached")
  public void schemaAgreementSuccess() {
    // Given
    StargateBridgeBlockingStub stub = makeBlockingStub();
    when(connection.isInSchemaAgreement()).thenReturn(true);
    mockAnyQueryAsSchemaChange();
    when(persistence.newConnection()).thenReturn(connection);
    startServer(persistence);

    // When
    QueryOuterClass.Response response = executeQuery(stub, "mock query");

    // Then
    assertThat(response.hasSchemaChange()).isTrue();
  }

  @Test
  @DisplayName("Should fail if schema agreement can't be reached")
  public void schemaAgreementFailure() {
    // Given
    StargateBridgeBlockingStub stub = makeBlockingStub();
    when(connection.isInSchemaAgreement()).thenReturn(false);
    mockAnyQueryAsSchemaChange();
    when(persistence.newConnection()).thenReturn(connection);
    startServer(persistence);

    // Then
    assertThatThrownBy(() -> executeQuery(stub, "mock query"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessage("DEADLINE_EXCEEDED: Failed to reach schema agreement after 400 milliseconds.");
  }

  private void mockAnyQueryAsSchemaChange() {
    Result.Prepared prepared =
        new Result.Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(),
            true,
            false);
    when(connection.prepare(any(String.class), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));
    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new Result.SchemaChange(
                    new Result.SchemaChangeMetadata(
                        "CREATED", "KEYSPACE", "ks1", null, Collections.emptyList()))));
  }
}
