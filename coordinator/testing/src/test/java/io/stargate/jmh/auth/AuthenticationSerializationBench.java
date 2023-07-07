package io.stargate.jmh.auth;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.db.AuthenticatedUser;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks for {@link AuthenticationSubject} serialization.
 *
 * <p>Run with: <code>
 * ../mvnw jmh:benchmark -Djmh.benchmarks=AuthenticationSerializationBench -Djmh.prof=gc</code>,
 * gave these results:
 *
 * <p><code>
 * Benchmark                                                                       (propertyCount)   Mode  Cnt     Score     Error   Units
 * AuthenticationSerializationBench.loadAuthenticationSubject                                    2  thrpt    5     3.384 ±   0.230  ops/us
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.alloc.rate                     2  thrpt    5  1807.395 ± 123.001  MB/sec
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.alloc.rate.norm                2  thrpt    5   560.000 ±   0.001    B/op
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.count                          2  thrpt    5   338.000            counts
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.time                           2  thrpt    5   228.000                ms
 * AuthenticationSerializationBench.loadAuthenticationSubject                                    4  thrpt    5     2.137 ±   0.029  ops/us
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.alloc.rate                     4  thrpt    5  1728.442 ±  23.497  MB/sec
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.alloc.rate.norm                4  thrpt    5   848.000 ±   0.001    B/op
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.count                          4  thrpt    5   333.000            counts
 * AuthenticationSerializationBench.loadAuthenticationSubject:·gc.time                           4  thrpt    5   227.000                ms
 * </code>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
public class AuthenticationSerializationBench {

  private Map<String, ByteBuffer> payload;

  @Param({"2", "4"})
  int propertyCount;

  @Setup(Level.Trial)
  public void setup() {
    Map<String, String> properties = new HashMap<>();
    for (int i = 0; i < propertyCount; i++) {
      properties.put(
          RandomStringUtils.randomAlphanumeric(10), RandomStringUtils.randomAlphanumeric(10));
    }

    AuthenticatedUser user = AuthenticatedUser.of("role", "token", true, properties);
    payload = AuthenticatedUser.Serializer.serialize(user);
  }

  @Benchmark
  public void loadAuthenticationSubject(Blackhole bh) {
    AuthenticatedUser user = AuthenticatedUser.Serializer.load(payload);
    AuthenticationSubject subject = AuthenticationSubject.of(user);
    bh.consume(subject);
  }
}
