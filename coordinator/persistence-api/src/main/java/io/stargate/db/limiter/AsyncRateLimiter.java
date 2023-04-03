package io.stargate.db.limiter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * An asynchronous rate limiter.
 *
 * <p>Conceptually, this provide functionality similar to <a
 * href="https://guava.dev/releases/snapshot-jre/api/docs/com/google/common/util/concurrent/RateLimiter.html">Guava
 * RateLimiter</a>, but can be used when asynchronicity is desired and blocking is not an option.
 * Instead of blocking to obtain "permits", if permits cannot be obtained immediately (if execution
 * needs to be delayed to enforce the rate limit), then the task is scheduled for later execution on
 * a predefined executor.
 *
 * <p>When a task is "submitted", it is allowed to proceed when permits are available to start it,
 * and its share of the work is reflected in the delay applied to the next task. This is done to
 * avoid unnecessarily delaying the very first task, if it happens to be started immediately after
 * creating the limiter. Because of this the limit can be temporarily overrun (i.e. the delta in
 * work between the start time can be greater than the rate times the time delta) but makes no
 * material difference (over waiting for the requested permits to start a task) in the rate-keeping
 * long term.
 *
 * <p>The limitation works by tracking the time at which we can execute a given piece of work by
 * growing the target time by the ratio between the requested work units and the rate. If that
 * target time is in the past, we can immediately run the work. If it is in the future, performing
 * the next piece of work should be delayed by the difference.
 *
 * <p>We also limit the amount of time the work is allowed to keep "in reserve", i.e. time for which
 * no work was actually done, to make sure that prolonged periods of no activity do not cause the
 * job to run unthrottled. This is done by adjusting the target time if it becomes too far in the
 * past.
 *
 * <p>The reserve window should be short but, to avoid losing permits due to slow processing
 * periods, it must be longer than the period of time that a unit of work is expected to take.
 *
 * <p>Note: if you make any changes to this file, please make sure to run AsyncRateLimiterTest
 * multiple times locally. On CI infrastructure the test often fails to get the right timings and
 * may pass without verifying correctness.
 */
public class AsyncRateLimiter {
  /** The executor on which tasks required delaying are scheduled. */
  private final ScheduledExecutorService executor;

  /**
   * Threshold under which a task is executed/enqueued immediately instead of being scheduled. Set
   * to 1 millisecond because the typical scheduling resolution is in milliseconds.
   */
  private final long schedulingThresholdNanos;

  /**
   * The minimum time that has to elapse for a permit to be granted, in nanoseconds. In other words,
   * the inverse of the permitted rate.
   */
  private volatile double nanosPerPermit;

  /**
   * The amount of time we are allowed to have "in reserve", i.e. elapsed time that we can use for
   * new permits. This is limited to make sure that we don't run unlimited if the process has not
   * been active for some time, or has not needed all of the provided rate.
   */
  private final long reserveWindowNanos;

  /**
   * The time to which permits have already been allocated. Every allocation increases this by
   * {@code permits * nanosPerPermits}, and the associated operation is scheduled for the resulting
   * time (if it is in the future). If we are running at capacity this would be in the future, and
   * if we are not this may be in the past. If it gets more than reserveWindowNanos nanoseconds
   * behind the current time, the next allocation will adjust it to make sure we do not allow long
   * unlimited runs due to long periods of inactivity.
   */
  private final AtomicLong consumedToTime;

  /**
   * Constructs a limiter.
   *
   * @param executor the executor on which tasks that are rate limited are scheduled.
   * @param rate the number of permits to allow per rateUnit.
   * @param rateUnit time unit for {@code rate}.
   * @param reserveWindow the amount of time we are allowed keep in reserve for work that is not yet
   *     claimed; this should be several times the time a unit of work is expected to take, so that
   *     the job cannot lose permits because of long-running tasks while it is operating at
   *     capacity.
   * @param reserveWindowUnit time unit for the reserve window
   * @param schedulingThreshold a time under which a task is executed directly instead of being
   *     scheduler. Meaning, if when {@link #acquireAndExecute} is called, the time before execution
   *     is computed to a value below this threshold, then the task is executed directly instead of
   *     being scheduled on the executor. This is meant to save the cost of scheduling for very
   *     small delays.
   * @param schedulingThresholdUnit time unit for schedulingThreshold.
   */
  public AsyncRateLimiter(
      ScheduledExecutorService executor,
      long rate,
      TimeUnit rateUnit,
      long reserveWindow,
      TimeUnit reserveWindowUnit,
      long schedulingThreshold,
      TimeUnit schedulingThresholdUnit) {
    this.executor = executor;
    this.nanosPerPermit = rateUnit.toNanos(1) * 1.0 / rate;
    this.reserveWindowNanos = reserveWindowUnit.toNanos(reserveWindow);
    this.consumedToTime = new AtomicLong(System.nanoTime());
    this.schedulingThresholdNanos = schedulingThresholdUnit.toNanos(schedulingThreshold);
  }

  /**
   * Constructs a limiter.
   *
   * <p>This constructor uses a default "scheduling threshold" of 1 milliseconds.
   *
   * @param executor the executor on which tasks that are rate limited are scheduled.
   * @param rate the number of permits to allow per {@code rateUnit}.
   * @param rateUnit time unit for {@code rate}.
   * @param reserveWindow the amount of time we are allowed keep in reserve for work that is not yet
   *     claimed; this should be several times the time a unit of work is expected to take, so that
   *     the job cannot lose permits because of long-running tasks while it is operating at
   *     capacity.
   * @param reserveWindowUnit time unit for the reserve window
   */
  public AsyncRateLimiter(
      ScheduledExecutorService executor,
      long rate,
      TimeUnit rateUnit,
      long reserveWindow,
      TimeUnit reserveWindowUnit) {
    this(executor, rate, rateUnit, reserveWindow, reserveWindowUnit, 1, TimeUnit.MILLISECONDS);
  }

  /**
   * Update the rate of the this limiter.
   *
   * @param rate the new rate (number of permits to allow per {@code rateUnit}).
   * @param rateUnit time unit for {@code rate}.
   */
  public void setRate(long rate, TimeUnit rateUnit) {
    this.nanosPerPermit = rateUnit.toNanos(1) * 1.0 / rate;
  }

  /**
   * The rate of this limiter.
   *
   * @param rateUnit the time unit in which to return the rate.
   * @return this limiter rate (number of permits allowed per {@code rateUnit}).
   */
  public long getRate(TimeUnit rateUnit) {
    return (long) (rateUnit.toNanos(1) * 1.0 / nanosPerPermit);
  }

  /**
   * Acquire the given number of permits and return the time in nanoseconds for which the works
   * should be scheduled.
   */
  private long acquire(long permits, long currentTimeNanos) {
    // Do not delay if no permits are requested, even if late. 0 work is already accounted for in
    // the previous
    // acquire call.
    if (permits <= 0) return currentTimeNanos;

    long scheduleTime;
    long timeToAcquire = (long) (permits * nanosPerPermit);

    while (true) {
      long consumedTo = consumedToTime.get();
      // scheduleTime is the time at which the task is allowed to start, which can be both in the
      // future (if running at limit) or in the past.
      // Make sure it's not more than the reserve window in the past though.
      scheduleTime = Math.max(consumedTo, currentTimeNanos - reserveWindowNanos);
      if (consumedToTime.compareAndSet(consumedTo, scheduleTime + timeToAcquire)) {
        return scheduleTime;
      } // Else we have had a concurrent modification. Retry.
    }
  }

  /**
   * Reserves the given number of permits and executes the provided asynchronous task when they
   * become available.
   *
   * <p>This may mean executing the task immediately on the current thread (if permits are already
   * available) or picking a time for which it is scheduled.
   *
   * @param permits the number of permits to acquire.
   * @param task an asynchronous task.
   * @return returns the number of nanoseconds the task has to wait before being executed.
   */
  public <T> CompletableFuture<T> acquireAndExecute(
      long permits, Supplier<CompletableFuture<T>> task) {
    long currentTime = System.nanoTime();
    long scheduleTime = acquire(permits, currentTime);
    long delay = scheduleTime - currentTime;

    if (delay < schedulingThresholdNanos) {
      // Time is in the past, or very close in the future. Execute immediately.
      return task.get();
    } else {
      // Time is in the future. Delay running the task.
      CompletableFuture<T> executionFuture = new CompletableFuture<>();
      executor.schedule(
          () -> {
            task.get().whenComplete((v, ex) -> complete(executionFuture, v, ex));
          },
          delay,
          TimeUnit.NANOSECONDS);
      return executionFuture;
    }
  }

  private static <T> void complete(CompletableFuture<T> toComplete, T result, Throwable exception) {
    if (exception != null) {
      toComplete.completeExceptionally(exception);
    } else {
      toComplete.complete(result);
    }
  }
}
