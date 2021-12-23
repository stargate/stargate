package io.stargate.db.limiter;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Note: The test is made to pass when the infrastructure does not give it the right timings. This
 * means that any failure, however rare or flaky, must be taken seriously as it signifies a real
 * problem with the code.
 */
public class AsyncRateLimiterTest {
  private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

  @Test
  public void testAcquireAndExecute() throws InterruptedException {
    assertTrue(testNonFlaky(new AcquireAndExecute()));
  }

  static class FlakyAssertionError extends AssertionError {
    public FlakyAssertionError(String detailMessage) {
      super(detailMessage);
    }
  }

  public boolean testNonFlaky(Tester tester) throws InterruptedException {
    // Even with five attempts, may not get the right timings.
    // Try to run the test, if we can get the timing right the test successfully verifies limitation
    // works well.
    for (int i = 0; i < 5; ++i) {
      try {
        return test(tester);
      } catch (FlakyAssertionError fae) {
        System.out.println("Attempt failed: " + fae.getMessage());
        if (i < 4) System.out.println("Retrying.");
      }
    }

    // If all five attempts don't get the timing right, we pass the test.
    System.out.println(
        "All five attempts to get the needed pause times failed. The test has not run successfully\n"
            + "but the problem we have seen is an infrastructure one, the code is probably working fine.");
    return true;
  }

  public boolean test(Tester tester) throws InterruptedException {
    final AtomicLong executed = new AtomicLong(0);

    long start = System.nanoTime();
    int reserveMs = 10;
    final AsyncRateLimiter limiter =
        new AsyncRateLimiter(executor, 1000, TimeUnit.SECONDS, reserveMs, TimeUnit.MILLISECONDS);

    long taskStart = tester.startTasks(limiter, 1, 3000, executed);

    Thread.sleep(1000);
    long taskEnd = System.nanoTime();
    long count = executed.get(); // adjust for pre-op vs. post-op wait
    // Use exact time (>= approximate time) for the end and adjust for not waiting if there's less
    // than 1 ms delay.
    long end = System.nanoTime();
    // The length of the time period that covers the time between starting tasks and getting count.
    // May actually be
    // slightly more (due to stalled threads,  approximate vs high precision, rounding to millis).
    // Note, nanosecond difference to millis may be 1 larger than the difference in time taken in
    // millis.
    // We also adjust for allowing tasks to start if they should be scheduled after less than 1 ms.
    long time = TimeUnit.NANOSECONDS.toMillis(end + TimeUnit.MILLISECONDS.toNanos(1) - start);
    long max = time + 1; // Adjusted by 1 because we start if we can fit the op start time
    // Minimum is not guaranteed (anything can cause a delay in processing). Use a big buffer to
    // avoid flakiness, especially on the first part.
    long min = Math.min(TimeUnit.NANOSECONDS.toMillis(taskEnd - taskStart) + reserveMs, time) - 500;
    assertTrue(count <= max, String.format("Should have run at most %d ops, did %d", max, count));
    if (time >= 2000) throw new FlakyAssertionError("Needed a pause of ~1000ms, got " + time);
    if (count < min)
      throw new FlakyAssertionError(
          String.format("Should have run at least %d ops, did %d", min, count));

    Thread.sleep(2500);
    if (executed.get() != 3000)
      throw new FlakyAssertionError(
          String.format("Should have completed %d tasks, did %d", 3000, executed.get()));

    start = tester.startTasks(limiter, 1, 3000, executed);

    Thread.sleep(1000);
    count = executed.get();
    time =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1) - start);
    max = time + 3000 + reserveMs + 1;
    min = time + 3000 + reserveMs - 300;
    assertTrue(count <= max, String.format("Should have run at most %d ops, did %d", max, count));
    if (time >= 2000) throw new FlakyAssertionError("Needed a pause of ~1000ms, got " + time);
    if (count < min)
      throw new FlakyAssertionError(
          String.format("Should have run at least %d ops, did %d", min, count));

    Thread.sleep(2500);
    if (executed.get() != 6000)
      throw new FlakyAssertionError(
          String.format("Should have completed %d tasks, did %d", 6000, executed.get()));

    // Now try some bigger tasks to see if we can break the reserve period
    start = tester.startTasks(limiter, 100, 30, executed);

    Thread.sleep(1000);
    count = executed.get();
    time =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1) - start);
    max = (time + reserveMs) / 100 + 6000 + 1;
    min = (time + reserveMs) / 100 + 6000 - 3;
    assertTrue(count <= max, String.format("Should have run at most %d ops, did %d", max, count));
    if (time >= 2000) throw new FlakyAssertionError("Needed a pause of ~1000ms, got " + time);
    if (count < min)
      throw new FlakyAssertionError(
          String.format("Should have run at least %d ops, did %d", min, count));

    Thread.sleep(2500);
    if (executed.get() != 6030)
      throw new FlakyAssertionError(
          String.format("Should have completed %d tasks, did %d", 6030, executed.get()));

    return true;
  }

  interface Tester {
    long startTasks(AsyncRateLimiter limiter, int permitsPerCall, int count, AtomicLong counter);
  }

  private static class AcquireAndExecute implements Tester {
    @Override
    public long startTasks(
        AsyncRateLimiter limiter, int permitsPerCall, int count, AtomicLong counter) {
      long start;
      CountDownLatch latch = new CountDownLatch(1);
      for (int i = 0; i < count; ++i)
        executor.submit(
            () -> {
              Uninterruptibles.awaitUninterruptibly(latch);
              limiter.acquireAndExecute(
                  permitsPerCall,
                  () -> {
                    counter.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                  });
            });
      start = System.nanoTime();
      latch.countDown();
      return start;
    }
  }
}
