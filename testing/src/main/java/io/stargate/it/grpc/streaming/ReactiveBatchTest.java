package io.stargate.it.grpc.streaming;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.grpc.GrpcIntegrationTest;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.ReactorStargateGrpc;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class ReactiveBatchTest extends GrpcIntegrationTest {

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE test");
  }

  @Test
  public void simpleReactiveBiDirectionalQueries(@TestKeyspace CqlIdentifier keyspace) {
    ReactorStargateGrpc.ReactorStargateStub stub = reactiveStubWithCallCredentials();

    Flux<QueryOuterClass.Batch> insertQueries =
        Flux.create(
            emitter -> {
              emitter.next(
                  QueryOuterClass.Batch.newBuilder()
                      .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                      .addQueries(
                          cqlBatchQuery(
                              "INSERT INTO test (k, v) VALUES (?, ?)",
                              Values.of("b"),
                              Values.of(2)))
                      .setParameters(batchParameters(keyspace))
                      .build());
              emitter.next(
                  QueryOuterClass.Batch.newBuilder()
                      .addQueries(
                          cqlBatchQuery(
                              "INSERT INTO test (k, v) VALUES (?, ?)",
                              Values.of("c"),
                              Values.of(3)))
                      .setParameters(batchParameters(keyspace))
                      .build());
              emitter.complete(); // client-side complete signal
            });

    Flux<QueryOuterClass.StreamingResponse> responseFlux = stub.executeBatchStream(insertQueries);
    StepVerifier.create(responseFlux)
        .expectNextMatches(Objects::nonNull)
        .expectNextMatches(Objects::nonNull)
        .expectComplete()
        .verify();
    QueryOuterClass.Response response =
        stubWithCallCredentials()
            .executeQuery(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
    Assertions.assertThat(response.hasResultSet()).isTrue();
    QueryOuterClass.ResultSet rs = response.getResultSet();
    Assertions.assertThat(new HashSet<>(rs.getRowsList()))
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    rowOf(Values.of("a"), Values.of(1)),
                    rowOf(Values.of("b"), Values.of(2)),
                    rowOf(Values.of("c"), Values.of(3)))));
  }

  @Test
  public void reactiveServerSideErrorPropagationCompleteProcessing(
      @TestKeyspace CqlIdentifier keyspace) {
    ReactorStargateGrpc.ReactorStargateStub stub = reactiveStubWithCallCredentials();

    Flux<QueryOuterClass.Batch> wrongQuery =
        Flux.create(
            emitter -> {
              emitter.next(
                  QueryOuterClass.Batch.newBuilder()
                      .addQueries(cqlBatchQuery("INSERT INTO not_existing (k, v) VALUES ('a', 1)"))
                      .setParameters(batchParameters(keyspace))
                      .build());
              emitter.complete();
            });

    Flux<QueryOuterClass.StreamingResponse> responseFlux = stub.executeBatchStream(wrongQuery);
    StepVerifier.create(responseFlux)
        .expectNextMatches(
            r -> {
              Status status = r.getStatus();
              return status.getCode() == 3
                  && status
                      .getMessage()
                      .equals("INVALID_ARGUMENT: unconfigured table not_existing");
            })
        .expectComplete()
        .verify();
  }

  @Test
  public void reactiveClientSideErrorPropagation() {
    ReactorStargateGrpc.ReactorStargateStub stub = reactiveStubWithCallCredentials();

    Flux<QueryOuterClass.Batch> streamWithError =
        Flux.create(
            emitter -> {
              emitter.error(new IllegalArgumentException("some client side processing error"));
              emitter.complete(); // complete signal is ignored
            });

    Flux<QueryOuterClass.StreamingResponse> responseFlux = stub.executeBatchStream(streamWithError);
    StepVerifier.create(responseFlux)
        .expectErrorMatches(
            e ->
                e instanceof StatusRuntimeException
                    && e.getMessage().contains("CANCELLED: Cancelled by client"))
        .verify();
  }

  @Test
  public void completeEmptyStream() {
    ReactorStargateGrpc.ReactorStargateStub stub = reactiveStubWithCallCredentials();

    Flux<QueryOuterClass.Batch> emptyWithComplete = Flux.create(FluxSink::complete);

    Flux<QueryOuterClass.StreamingResponse> responseFlux =
        stub.executeBatchStream(emptyWithComplete);
    StepVerifier.create(responseFlux).expectComplete().verify();
  }
}
