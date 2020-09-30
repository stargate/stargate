package io.stargate.db.cdc;

import com.codahale.metrics.Clock;
import com.codahale.metrics.EWMA;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class DefaultCDCHealthChecker implements CDCHealthChecker {
  private double errorRateThreshold;
  private int minErrorsPerSecond;
  static final int INTERVAL = 5;
  static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(INTERVAL);

  private AutoTickEWMA successes;
  private AutoTickEWMA errors;

  /**
   * Creates a new instance of {@link DefaultCDCHealthChecker}.
   *
   * @param errorRateThreshold The percentage of error requests compared to total, expressed in as a
   *     double from 0 to 1.
   * @param minErrorsPerSecond The minimum amount of error occurrences per second for the health
   *     checker to consider the error ratio. This setting is created to prevent detecting the CDC
   *     producer as unhealthy when there's low traffic and few errors.
   * @param ewmaIntervalMinutes The interval to determine the coefficient for the degree of
   *     weighting decrease in the exponentially weighted moving average (EWMA). The health checker
   *     will use this value to set a soothing factor equivalent to UNIX load average.
   */
  DefaultCDCHealthChecker(
      double errorRateThreshold, int minErrorsPerSecond, int ewmaIntervalMinutes) {
    this(errorRateThreshold, minErrorsPerSecond, ewmaIntervalMinutes, TickClock.defaultClock);
  }

  @VisibleForTesting
  DefaultCDCHealthChecker(
      double errorRateThreshold, int minErrorsPerSecond, int ewmaIntervalMinutes, TickClock clock) {

    if (errorRateThreshold <= 0 || errorRateThreshold > 1) {
      throw new IllegalArgumentException(
          "Error rate threshold should be greater than 0 and lower than 1");
    }

    if (ewmaIntervalMinutes <= 0 || ewmaIntervalMinutes > 15) {
      throw new IllegalArgumentException(
          "The interval used to determine the smoothing factor for the exponentially "
              + "weighted moving average must be higher 0 and lower than 15 minutes");
    }

    this.errorRateThreshold = errorRateThreshold;
    this.minErrorsPerSecond = minErrorsPerSecond;
    successes = new AutoTickEWMA(ewmaIntervalMinutes, clock);
    errors = new AutoTickEWMA(ewmaIntervalMinutes, clock);
  }

  @Override
  public boolean isHealthy() {
    double errorRate = errors.getRate();
    if (errorRate < minErrorsPerSecond || errorRate == 0.0) {
      return true;
    }

    double successRate = successes.getRate();
    System.out.println(String.format("Error: %f; Success: %f", errorRate, successRate));
    double percentage = errorRate / (successRate + errorRate);
    return percentage < errorRateThreshold;
  }

  @Override
  public void reportSendError() {
    errors.update();
  }

  @Override
  public void reportSendSuccess() {
    successes.update();
  }

  interface TickClock {
    long getTick();

    TickClock defaultClock =
        new TickClock() {
          private final Clock clock = com.codahale.metrics.Clock.defaultClock();

          @Override
          public long getTick() {
            return clock.getTick();
          }
        };
  }

  @Override
  public void close() {}

  private static class AutoTickEWMA {
    private static final double SECONDS_PER_MINUTE = 60.0;
    private final AtomicLong lastTick;
    private final EWMA instance;
    private final TickClock clock;

    AutoTickEWMA(int minutes, TickClock clock) {
      this.clock = clock;
      // See com.codahale.metrics.EWMA for more information
      double alpha = 1 - Math.exp(-INTERVAL / SECONDS_PER_MINUTE / minutes);
      instance = new EWMA(alpha, INTERVAL, TimeUnit.SECONDS);
      lastTick = new AtomicLong(clock.getTick());
    }

    void update() {
      instance.update(1);
    }

    /** Gets the rate with seconds as unit of time. */
    double getRate() {
      tickIfNecessary();
      return instance.getRate(TimeUnit.SECONDS);
    }

    /** Updates the time passed for the EWMA. */
    private void tickIfNecessary() {
      final long oldTick = lastTick.get();
      final long newTick = clock.getTick();
      final long age = newTick - oldTick;
      if (age > TICK_INTERVAL) {
        final long newIntervalStartTick = newTick - age % TICK_INTERVAL;
        if (lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
          final long requiredTicks = age / TICK_INTERVAL;
          for (long i = 0; i < requiredTicks; i++) {
            instance.tick();
          }
        }
      }
    }
  }
}
