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
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class TracingQueryTest extends GrpcIntegrationTest {

  @Test
  public void tracingIdNormalQueryDisabled(@TestKeyspace CqlIdentifier keyspace) {
    // given
    StargateBlockingStub stub = stubWithCallCredentials();

    // when
    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace, false)));

    // then
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isEmpty();

    // when
    response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES (?, ?)",
                queryParameters(keyspace, false),
                Values.of("b"),
                Values.of(2)));
    // then
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isEmpty();

    // when
    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace, false)));

    // then
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getTracingId()).isEmpty();
  }

  @Test
  public void tracingIdNormalQueryEnabled(@TestKeyspace CqlIdentifier keyspace) {
    // given
    StargateBlockingStub stub = stubWithCallCredentials();

    // when
    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace, true)));

    // then
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isNotEmpty();

    // when
    response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES (?, ?)",
                queryParameters(keyspace, true),
                Values.of("b"),
                Values.of(2)));
    // then
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isNotEmpty();

    // when
    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace, true)));

    // then
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getTracingId()).isNotEmpty();
  }

  @Test
  public void tracingIdBatchQueryDisabled(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .setParameters(batchParameters(keyspace, false))
                .build());
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isEmpty();
  }

  @Test
  public void tracingIdBatchQueryEnabled(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .setParameters(batchParameters(keyspace, true))
                .build());
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isNotEmpty();
  }

  @Test
  public void tracingIdNormalQueryEnabledGetTracingData(@TestKeyspace CqlIdentifier keyspace) {
    // given
    StargateBlockingStub stub = stubWithCallCredentials();

    // when
    Response response =
        stub.executeQuery(
            cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace, true)));

    // then
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isNotEmpty();
    assertThat(response.getTracesList().size()).isGreaterThan(1);
    validateTrace(response);

    // when
    response =
        stub.executeQuery(
            cqlQuery(
                "INSERT INTO test (k, v) VALUES (?, ?)",
                queryParameters(keyspace, true),
                Values.of("b"),
                Values.of(2)));
    // then
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isNotEmpty();
    assertThat(response.getTracesList().size()).isGreaterThan(1);
    validateTrace(response);

    // when
    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace, true)));

    // then
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getTracingId()).isNotEmpty();
    assertThat(response.getTracesList().size()).isGreaterThan(1);
    validateTrace(response);
  }

  @Test
  public void tracingIdBatchQueryEnabledGetTracingData(@TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .setParameters(batchParameters(keyspace, true))
                .build());
    assertThat(response).isNotNull();
    assertThat(response.getTracingId()).isNotEmpty();
    assertThat(response.getTracesList().size()).isGreaterThan(1);
    validateTrace(response);
  }

  private void validateTrace(Response response) {
    QueryOuterClass.TraceEvent firstTrace = response.getTraces(0);
    assertThat(firstTrace.getActivity()).isNotEmpty();
    assertThat(firstTrace.getSource()).isNotEmpty();
    assertThat(firstTrace.getSourceElapsed()).isGreaterThan(0);
    assertThat(firstTrace.getThread()).isNotEmpty();
  }
}
