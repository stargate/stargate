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

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaOperationsTest extends BaseGrpcServiceTest {

  @Test
  @DisplayName("Should fail if system keyspace contents are not described correctly")
  public void schemaDescribeSuccess() {
    // Given
    StargateGrpc.StargateBlockingStub stub = makeBlockingStub();
    startServer(persistence);

    // When
    Schema.CqlKeyspaceDescribe response =
        stub.describeKeyspace(Schema.DescribeQuery.newBuilder().setKeyspaceName("system").build());

    // Then
    assertThat(response.getCqlKeyspace().getDurableWrites()).isTrue();
  }
}
