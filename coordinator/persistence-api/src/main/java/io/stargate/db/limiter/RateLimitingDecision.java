package io.stargate.db.limiter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.cassandra.stargate.exceptions.UnauthorizedException;

/**
 * The decision taken by a {@link RateLimitingManager} for a particular query.
 *
 * <p>The is essentially 3 possible decision:
 *
 * <ul>
 *   <li>to not rate limit at all ({@link #unlimited()}).
 *   <li>to rate limit the query, using a provided limiter ({@link #limit}).
 *   <li>to reject the query altogether ({@link #reject}).
 * </ul>
 */
public abstract class RateLimitingDecision {

  private RateLimitingDecision() {}

  /** Creates a new decision consisting of not rate limiting a query. */
  public static Unlimited unlimited() {
    return Unlimited.INSTANCE;
  }

  /**
   * Creates a new decision consisting of rate limiting a query through acquiring the provided
   * number of permit on the provided limiter.
   */
  public static Limited limit(AsyncRateLimiter limiter, long permitsToAcquire) {
    return new Limited(limiter, permitsToAcquire);
  }

  /**
   * Creates a new decision consisting of rejecting a query, the rejected query throwing an {@link
   * UnauthorizedException} with the provided message.
   */
  public static Rejected reject(String rejectionMessage) {
    return new Rejected(rejectionMessage);
  }

  /** Applies this decision to the provided asynchronous taks/query. */
  public abstract <T> CompletableFuture<T> apply(Supplier<CompletableFuture<T>> task);

  public static class Unlimited extends RateLimitingDecision {
    private static final Unlimited INSTANCE = new Unlimited();

    @Override
    public <T> CompletableFuture<T> apply(Supplier<CompletableFuture<T>> task) {
      return task.get();
    }
  }

  public static class Limited extends RateLimitingDecision {
    private final AsyncRateLimiter limiter;
    private final long permitsToAcquire;

    private Limited(AsyncRateLimiter limiter, long permitsToAcquire) {
      this.limiter = limiter;
      this.permitsToAcquire = permitsToAcquire;
    }

    @Override
    public <T> CompletableFuture<T> apply(Supplier<CompletableFuture<T>> task) {
      return limiter.acquireAndExecute(permitsToAcquire, task);
    }
  }

  public static class Rejected extends RateLimitingDecision {
    private final String rejectionMessage;

    private Rejected(String rejectionMessage) {
      this.rejectionMessage = rejectionMessage;
    }

    @Override
    public <T> CompletableFuture<T> apply(Supplier<CompletableFuture<T>> task) {
      CompletableFuture<T> exceptionalFuture = new CompletableFuture<>();
      exceptionalFuture.completeExceptionally(new UnauthorizedException(rejectionMessage));
      return exceptionalFuture;
    }
  }
}
