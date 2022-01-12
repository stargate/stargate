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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.grpc.GrpcIntegrationTest;
import io.stargate.proto.QueryOuterClass.Query;
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
public class ExecuteQueryStreamingTest extends GrpcIntegrationTest {

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE test");
  }

  @Test
  public void simpleStreamingQuery(@TestKeyspace CqlIdentifier keyspace) {
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

    StreamObserver<Query> requestObserver = stub.executeQueryStream(responseStreamObserver);

    requestObserver.onNext(
        cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));

    requestObserver.onNext(
        cqlQuery(
            "INSERT INTO test (k, v) VALUES (?, ?)",
            queryParameters(keyspace),
            Values.of("b"),
            Values.of(2)));

    // all inserted records may be not visible to the 3rd query (SELECT)
    // because all calls are non-blocking. Therefore, we need to wait for response of two insert
    // queries
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> responses.size() == 2);

    requestObserver.onNext(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    requestObserver.onCompleted();
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> responses.size() == 3);

    assertThat(responses.get(0)).isNotNull();
    assertThat(responses.get(1)).isNotNull();
    StreamingResponse response = responses.get(2);

    assertThat(response.getResponse().hasResultSet()).isTrue();
    ResultSet rs = response.getResponse().getResultSet();
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    rowOf(Values.of("a"), Values.of(1)), rowOf(Values.of("b"), Values.of(2)))));
  }

  @Test
  public void streamingQueryWithError(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
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

    StreamObserver<Query> requestObserver = stub.executeQueryStream(responseStreamObserver);

    requestObserver.onNext(
        cqlQuery("INSERT INTO not_existing (k, v) VALUES ('a', 1)", queryParameters(keyspace)));

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> responses.size() == 1);
    requestObserver.onCompleted();
    assertThat(error.get()).isNull();

    StreamingResponse streamingResponse = responses.get(0);
    Status status = streamingResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(3);
    assertThat(status.getMessage()).contains("INVALID_ARGUMENT");
    assertThat(status.getMessage()).contains("not_existing");
    assertThat(ErrorInfo.parseFrom(status.getDetails(0).getValue()).getReason())
        .contains("not_existing");
  }

  @Test
  public void streamingQueryWithNextAndError(@TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    AtomicReference<Throwable> error = new AtomicReference<>();
    List<StreamingResponse> responses = new CopyOnWriteArrayList<>();

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

    StreamObserver<Query> requestObserver = stub.executeQueryStream(responseStreamObserver);

    requestObserver.onNext(
        cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));

    requestObserver.onNext(
        cqlQuery("INSERT INTO not_existing (k, v) VALUES ('a', 1)", queryParameters(keyspace)));

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> responses.size() == 2);
    requestObserver.onCompleted();
    assertThat(error.get()).isNull();
    assertThat(error.get()).isNull();
    // gRPC guarantees message ordering within an individual RPC call.
    // Both onNext may be executed as a separate RPC calls, therefore we cannot be sure about the
    // order or responses.
    StreamingResponse streamingResponse =
        responses.stream().filter(v -> v.getStatus().getCode() == 3).findFirst().get();
    Status status = streamingResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(3);
    assertThat(status.getMessage()).contains("INVALID_ARGUMENT");
    assertThat(status.getMessage()).contains("not_existing");
    assertThat(ErrorInfo.parseFrom(status.getDetails(0).getValue()).getReason())
        .contains("not_existing");
  }
}
