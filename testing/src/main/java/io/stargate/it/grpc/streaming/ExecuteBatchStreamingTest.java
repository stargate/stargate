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
package io.stargate.it.grpc.streaming;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.rpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.grpc.GrpcIntegrationTest;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.StreamingResponse;
import io.stargate.proto.StargateGrpc.StargateStub;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class ExecuteBatchStreamingTest extends GrpcIntegrationTest {

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE test");
  }

  @Test
  public void simpleStreamingBatch(@TestKeyspace CqlIdentifier keyspace) {
    List<StreamingResponse> responses = new CopyOnWriteArrayList<>();

    StargateStub stub = asyncStubWithCallCredentials();
    StreamObserver<StreamingResponse> responseStreamObserver =
        new StreamObserver<StreamingResponse>() {
          @Override
          public void onNext(StreamingResponse value) {
            responses.add(value);
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };

    StreamObserver<Batch> requestObserver = stub.executeBatchStream(responseStreamObserver);

    requestObserver.onNext(
        Batch.newBuilder()
            .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
            .addQueries(
                cqlBatchQuery(
                    "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
            .setParameters(batchParameters(keyspace))
            .build());

    requestObserver.onNext(
        Batch.newBuilder()
            .addQueries(
                cqlBatchQuery(
                    "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
            .setParameters(batchParameters(keyspace))
            .build());

    // all inserted records may be not visible to the subsequent SELECT query
    // because all reactive calls are non-blocking. Therefore, we need to wait for response of two
    // batch insert
    // queries
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> responses.size() == 2);
    requestObserver.onCompleted();

    Response response =
        stubWithCallCredentials()
            .executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    rowOf(Values.of("a"), Values.of(1)),
                    rowOf(Values.of("b"), Values.of(2)),
                    rowOf(Values.of("c"), Values.of(3)))));
  }

  @Test
  public void streamingQueryWithError(@TestKeyspace CqlIdentifier keyspace) {
    List<StreamingResponse> responses = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> error = new AtomicReference<>();

    StargateStub stub = asyncStubWithCallCredentials();
    StreamObserver<StreamingResponse> responseStreamObserver =
        new StreamObserver<StreamingResponse>() {
          @Override
          public void onNext(StreamingResponse value) {
            responses.add(value);
          }

          @Override
          public void onError(Throwable t) {
            error.set(t);
          }

          @Override
          public void onCompleted() {}
        };

    StreamObserver<QueryOuterClass.Batch> requestObserver =
        stub.executeBatchStream(responseStreamObserver);

    requestObserver.onNext(
        Batch.newBuilder()
            .addQueries(cqlBatchQuery("INSERT INTO not_existing (k, v) VALUES ('a', 1)"))
            .setParameters(batchParameters(keyspace))
            .build());

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> responses.size() == 1);
    requestObserver.onCompleted();
    assertThat(error.get()).isNull();

    StreamingResponse streamingResponse = responses.get(0);
    Status status = streamingResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(3);
    assertThat(status.getMessage()).isEqualTo("unconfigured table not_existing");
  }

  @Test
  public void streamingBatchQueryWithNextAndError(@TestKeyspace CqlIdentifier keyspace) {
    List<StreamingResponse> responses = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> error = new AtomicReference<>();

    StargateStub stub = asyncStubWithCallCredentials();
    StreamObserver<StreamingResponse> responseStreamObserver =
        new StreamObserver<StreamingResponse>() {
          @Override
          public void onNext(StreamingResponse value) {
            responses.add(value);
          }

          @Override
          public void onError(Throwable t) {
            error.set(t);
          }

          @Override
          public void onCompleted() {}
        };

    StreamObserver<Batch> requestObserver = stub.executeBatchStream(responseStreamObserver);

    requestObserver.onNext(
        Batch.newBuilder()
            .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
            .setParameters(batchParameters(keyspace))
            .build());

    requestObserver.onNext(
        Batch.newBuilder()
            .addQueries(cqlBatchQuery("INSERT INTO not_existing (k, v) VALUES ('a', 1)"))
            .setParameters(batchParameters(keyspace))
            .build());

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> responses.size() == 2);
    requestObserver.onCompleted();
    assertThat(error.get()).isNull();
    assertThat(responses.get(0).getResponse()).isNotNull();

    // todo potential ordering problem
    StreamingResponse streamingResponse = responses.get(1);
    Status status = streamingResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(3);
    assertThat(status.getMessage()).isEqualTo("unconfigured table not_existing");
  }
}
