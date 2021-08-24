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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v uuid, PRIMARY KEY(k, v))",
    })
public class IdempotencyTest extends GrpcIntegrationTest {

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE test");
  }

  @Test
  public void simpleIdempotentQuery(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES ('a', 123e4567-e89b-12d3-a456-426614174000)",
                queryParameters(keyspace)));
    assertThat(response).isNotNull();
    assertThat(response.getIsIdempotent()).isTrue();
  }

  @Test
  public void simpleNonIdempotentQuery(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', uuid())", queryParameters(keyspace)));
    assertThat(response).isNotNull();
    assertThat(response.getIsIdempotent()).isFalse();
  }

  @Test
  public void simpleBatchIdempotent(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES ('a', 123e4567-e89b-12d3-a456-426614174000)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)",
                        Values.of("b"),
                        Values.of("123e4567-e89b-12d3-a456-426614174001")))
                .setParameters(batchParameters(keyspace))
                .build());
    assertThat(response).isNotNull();
    assertThat(response.getIsIdempotent()).isTrue();
  }

  @Test
  public void simpleBatchNonIdempotent(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES ('a', 123e4567-e89b-12d3-a456-426614174001)"))
                .addQueries(
                    cqlBatchQuery("INSERT INTO test (k, v) VALUES (?, uuid())", Values.of("b")))
                .setParameters(batchParameters(keyspace))
                .build());
    assertThat(response).isNotNull();
    assertThat(response.getIsIdempotent()).isFalse();
  }
}
