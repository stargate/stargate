package io.stargate.db.limiter.global.impl;

import static java.lang.String.format;

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Batch;
import io.stargate.db.ClientInfo;
import io.stargate.db.Parameters;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import io.stargate.db.limiter.AsyncRateLimiter;
import io.stargate.db.limiter.RateLimitingDecision;
import io.stargate.db.limiter.RateLimitingManager;
import io.stargate.db.limiter.RateLimitingManager.ConnectionManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A rate limiting manager that rate limit all queries globally (except reads on system tables) to a
 * pre-configured amount of queries per seconds.
 *
 * <p>The enforced QPS is defined through the {@link #RATE_PROPERTY} system property.
 *
 * <p>As of this writing, this limiter serves mostly for testing (see the {@code
 * GlobalRateLimitingTest}) and as a demonstration of how to write a simple {@link
 * RateLimitingManager}. However, it might need some improvements to be truly usable in production.
 * Mainly, the rate cannot be currently configured at runtime, which is limiting (pun intended).
 * Additionally, this avoid (by default) limiting some reads on system tables, but the way this is
 * done is fragile/inflexible.
 */
public class GlobalRateLimitingManager implements RateLimitingManager, ConnectionManager {
  /*
   * Currently, the rate is set by a system property at startup and can't change, which makes
   * this not that usable in real life. Of course, the fact that it is a pretty big hammer also
   * limits the usefulness. So this is currently more for 1) testing rate limiting and 2)
   * demonstrates how one can write a `RateLimitingManager`.
   * But at least, making it possible to change the rate at runtime would be useful (though "how"
   * remains a question to be answered).
   */

  public static final String RATE_PROPERTY = "stargate.limiter.global.rate_qps";
  private static final int DELAYED_TASKS_EXECUTOR_THREADS =
      Integer.getInteger("stargate.limiter.global.threads", 4);
  private static final boolean RATE_LIMIT_SYSTEM_TABLES =
      Boolean.getBoolean("stargate.limiter.global.enabled_on_system_tables");

  private final AsyncRateLimiter limiter;

  public GlobalRateLimitingManager() {
    this(buildLimiter(defaultExecutor()));
  }

  public GlobalRateLimitingManager(AsyncRateLimiter limiter) {
    this.limiter = limiter;
  }

  private static ScheduledExecutorService defaultExecutor() {
    return Executors.newScheduledThreadPool(DELAYED_TASKS_EXECUTOR_THREADS);
  }

  private static AsyncRateLimiter buildLimiter(ScheduledExecutorService executor) {
    String rateStr = System.getProperty(RATE_PROPERTY);
    if (rateStr == null || rateStr.isEmpty()) {
      throw new IllegalArgumentException(
          format(
              "Global rate limiting is enabled but missing (or empty) value for property '%s'",
              RATE_PROPERTY));
    }

    try {
      return new AsyncRateLimiter(
          executor, Long.parseLong(rateStr), TimeUnit.SECONDS, 1, TimeUnit.MINUTES);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          format(
              "Invalid value for property '%s': " + "expected a number, but got %s",
              RATE_PROPERTY, rateStr));
    }
  }

  @Override
  public String description() {
    return format("global rate limiting at %d queries/seconds", limiter.getRate(TimeUnit.SECONDS));
  }

  @Override
  public ConnectionManager forNewConnection() {
    // Nothing is specific to connection for this rate limiter
    return this;
  }

  @Override
  public ConnectionManager forNewConnection(ClientInfo clientInfo) {
    // Nothing is specific to connection for this rate limiter
    return this;
  }

  @Override
  public void onUserLogged(AuthenticatedUser user) {
    // Nothing specific to users.
  }

  private static boolean shouldExclude(String query) {
    // TODO: this is a horrible horrible hack. As we only pass strings through `Persistence`, we
    //   have no simple reliable way to know the keyspace or table queried. But if we want to use
    //   this for testing somewhat reliably, we need to be able to exclude the bunch of system table
    //   reads that drivers do on connection (basically, for testing to be reliable, we want to set
    //   a  very aggressive rate limiting, but if driver queries hit it, the test will take
    // forever).
    //   So we explicitly target those driver system table reads. It's super fragile because 1) it
    //   assume the driver don't add extra spaces compared to what we check and 2) it doesn't use
    //   prepared queries (if it did, we wouldn't rate limit the prepare, but we would the
    //   execution).
    //   Tl;dr: we'd need Stargate to stop passing raw strings into the persistence to be able to
    //   do this properly.
    return !RATE_LIMIT_SYSTEM_TABLES && query.startsWith("SELECT") && query.contains("FROM System");
  }

  @Override
  public RateLimitingDecision forPrepare(String query, Parameters parameters) {
    if (shouldExclude(query)) {
      return RateLimitingDecision.unlimited();
    }
    return RateLimitingDecision.limit(limiter, 1);
  }

  @Override
  public RateLimitingDecision forExecute(Statement statement, Parameters parameters) {
    if (statement instanceof SimpleStatement
        && shouldExclude(((SimpleStatement) statement).queryString())) {
      return RateLimitingDecision.unlimited();
    }
    return RateLimitingDecision.limit(limiter, 1);
  }

  @Override
  public RateLimitingDecision forBatch(Batch batch, Parameters parameters) {
    return RateLimitingDecision.limit(limiter, batch.size());
  }
}
