package io.stargate.sgv2.api.common.config;

import io.smallrye.config.WithDefault;
import java.time.Duration;

/**
 * Configuration class used for defining how specific call may be retried, by limiting number of
 * calls, total time spent on all calls and delays between calls; delays between calls and so on.
 *
 * <p>Maximum call count and maximum total time are both used as limits so the retry process ends if
 * either is exceeded.
 *
 * <p>Delays are calculated start-to-start so that time actually spent on calls themselves are
 * removed from delay added between the calls.
 */
public interface RetryableCallsConfig {
  static final String DEFAULT_MAX_CALLS = "10";
  static final String DEFAULT_MAX_TIME = "PT5M";
  static final String DEFAULT_INITIAL_DELAY = "PT3S";
  static final String DEFAULT_MAX_DELAY = "PT10S";
  static final String DEFAULT_DELAY_RATIO = "1.5";

  /**
   * Maximum calls to make to fetch metadata before giving up.
   *
   * <p>Defaults to 10 total calls.
   */
  @WithDefault(DEFAULT_MAX_CALLS)
  int maxCalls();

  /**
   * Maximum total time for all calls (and delay between calls) to fetch metadata before giving up.
   *
   * <p>Defaults to 5 minutes.
   */
  @WithDefault(DEFAULT_MAX_TIME)
  Duration maxTime();

  /**
   * Amount of delay between the initial (failed) call and the first retry.
   *
   * <p>Defaults to 3 seconds.
   */
  @WithDefault(DEFAULT_INITIAL_DELAY)
  Duration initialDelay();

  /**
   * Maximum amount of delay between any two calls.
   *
   * <p>Defaults to 10 seconds.
   */
  @WithDefault(DEFAULT_MAX_DELAY)
  Duration maxDelay();

  /**
   * Ratio by which to multiply delay.
   *
   * <p>Defaults to {@code 1.5} which means that delay increased by 50% each time (1.5 meaning new
   * value is 150% of current value). Note: actual delay subject to the limit imposed by {@link
   * #maxDelay()}}.
   */
  @WithDefault(DEFAULT_DELAY_RATIO)
  double delayRatio();

  /** We also include an actual implementation to use by tests. */
  public record Impl(
      int maxCalls, Duration maxTime, Duration initialDelay, Duration maxDelay, double delayRatio)
      implements RetryableCallsConfig {
    public Impl() {
      this(
          Integer.parseInt(DEFAULT_MAX_CALLS),
          Duration.parse(DEFAULT_MAX_TIME),
          Duration.parse(DEFAULT_INITIAL_DELAY),
          Duration.parse(DEFAULT_MAX_DELAY),
          Double.parseDouble(DEFAULT_DELAY_RATIO));
    }
  }
}
