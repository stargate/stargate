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
    assertThat(response.getTraces().getId()).isEmpty();

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
    assertThat(response.getTraces().getId()).isEmpty();

    // when
    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace, false)));

    // then
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getTraces().getId()).isEmpty();
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
    assertThat(response.getTraces()).isNotNull();

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
    assertThat(response.getTraces()).isNotNull();

    // when
    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace, true)));

    // then
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getTraces()).isNotNull();
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
    assertThat(response.getTraces().getId()).isEmpty();
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
    assertThat(response.getTraces()).isNotNull();
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
    assertThat(response.getTraces()).isNotNull();
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
    assertThat(response.getTraces()).isNotNull();
    validateTrace(response);

    // when
    response = stub.executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace, true)));

    // then
    assertThat(response.hasResultSet()).isTrue();
    assertThat(response.getTraces()).isNotNull();
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
    assertThat(response.getTraces()).isNotNull();
    validateTrace(response);
  }

  private void validateTrace(Response response) {
    QueryOuterClass.Traces traces = response.getTraces();
    assertThat(traces.getDuration()).isGreaterThan(0);
    assertThat(traces.getStartedAt()).isGreaterThan(0);
    assertThat(traces.getId()).isNotNull();

    assertThat(traces.getEventsList())
        .isNotEmpty()
        .allSatisfy(
            event -> {
              assertThat(event.getEventId()).isNotEmpty();
              assertThat(event.getActivity()).isNotEmpty();
              assertThat(event.getThread()).isNotEmpty();
              assertThat(event.getSource()).isNotEmpty();
              assertThat(event.getSourceElapsed()).isGreaterThanOrEqualTo(0);
            });
  }
}
