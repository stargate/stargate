package io.stargate.it.limiter;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.stargate.db.DbActivator;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.TestOrder;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {"CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))"})
@Order(TestOrder.LAST)
public class GlobalRateLimitingTest extends BaseIntegrationTest {

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.putSystemProperties(DbActivator.RATE_LIMITING_ID_PROPERTY, "GlobalRateLimiting");
    builder.putSystemProperties("stargate.limiter.global.rate_qps", "1");
  }

  @Test
  @DisplayName("Should rate limit queries")
  public void testRateLimited(CqlSession session) {
    int QUERIES = 5;
    // We're executing a point query on an empty table 5 times, so this should be really fast, even
    // on a constrained environment, if rate limiting does not work. If it works though, as we've
    // limited to 1 query per second, it should take at least 3 seconds (the 1 query don't get
    // limited at all, and we round the numbers of nanos, plus we want to avoid bad timings so...).
    long start = System.nanoTime();
    for (int i = 0; i < QUERIES; i++) {
      ResultSet resultSet = session.execute("SELECT * FROM test WHERE k = 'foo'");
      assertThat(resultSet).hasSize(0);
    }
    long elapsedNanos = System.nanoTime() - start;
    assertThat(TimeUnit.NANOSECONDS.toSeconds(elapsedNanos)).isGreaterThanOrEqualTo(QUERIES - 2);
  }
}
