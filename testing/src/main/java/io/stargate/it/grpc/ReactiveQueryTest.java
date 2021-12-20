package io.stargate.it.grpc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import io.stargate.grpc.Values;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.ReactorStargateGrpc;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
    })
public class ReactiveQueryTest extends GrpcIntegrationTest {

  @AfterEach
  public void cleanup(CqlSession session) {
    session.execute("TRUNCATE TABLE test");
  }

  @Test
  public void simpleReactiveBiDirectionalQueries(@TestKeyspace CqlIdentifier keyspace)
      throws InterruptedException {
    ReactorStargateGrpc.ReactorStargateStub stub = reactiveStubWithCallCredentials();
    // due to async nature of all reactive queries, we should not mix selects and inserts
    // because they may interleave

    Flux<QueryOuterClass.Query> insertQueries =
        Flux.create(
            emitter -> {
              emitter.next(
                  cqlQuery("INSERT INTO test (k, v) VALUES ('a', 1)", queryParameters(keyspace)));
              emitter.next(
                  cqlQuery(
                      "INSERT INTO test (k, v) VALUES (?, ?)",
                      queryParameters(keyspace),
                      Values.of("b"),
                      Values.of(2)));
              emitter.complete(); // client-side complete signal
            });

    Flux<QueryOuterClass.Query> selectQueries =
        Flux.create(
            emitter -> {
              emitter.next(cqlQuery("SELECT * FROM test", queryParameters(keyspace)));
              emitter.complete(); // client-side complete signal
            });

    Flux<QueryOuterClass.Response> responseFlux = stub.executeQueryStream(insertQueries);
    StepVerifier.create(responseFlux)
        .expectNextMatches(Objects::nonNull)
        .expectNextMatches(Objects::nonNull)
        .expectComplete()
        .verify();

    responseFlux = stub.executeQueryStream(selectQueries);
    StepVerifier.create(responseFlux)
        .expectNextMatches(
            response -> {
              assertThat(response.hasResultSet()).isTrue();
              QueryOuterClass.ResultSet rs = response.getResultSet();
              assertThat(new HashSet<>(rs.getRowsList()))
                  .isEqualTo(
                      new HashSet<>(
                          Arrays.asList(
                              rowOf(Values.of("a"), Values.of(1)),
                              rowOf(Values.of("b"), Values.of(2)))));
              return rs.getRowsList().size() == 2;
            })
        .expectComplete()
        .verify();
  }
}
