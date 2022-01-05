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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
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
    List<Response> responses = new CopyOnWriteArrayList<>();

    StargateStub stub = asyncStubWithCallCredentials();
    StreamObserver<Response> responseStreamObserver =
        new StreamObserver<Response>() {
          @Override
          public void onNext(Response value) {
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
    Response response = responses.get(2);

    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    rowOf(Values.of("a"), Values.of(1)), rowOf(Values.of("b"), Values.of(2)))));
  }

  @Test
  public void streamingQueryWithError(@TestKeyspace CqlIdentifier keyspace) {
    AtomicReference<Throwable> error = new AtomicReference<>();

    StargateStub stub = asyncStubWithCallCredentials();
    StreamObserver<Response> responseStreamObserver =
        new StreamObserver<Response>() {
          @Override
          public void onNext(Response value) {}

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

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> error.get() != null);
    Throwable throwable = error.get();
    assertThat(throwable).isInstanceOf(StatusRuntimeException.class);
    assertThat(throwable.getMessage()).contains("INVALID_ARGUMENT").contains("not_existing");
  }
}
