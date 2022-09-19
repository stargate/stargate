package io.stargate.sgv2.api.common.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;

/**
 * Configuration value definition for configuring aspects of Bridge Bootstrapping which fetches
 * Persistence System capabilities metadata during startup of Services).
 *
 * <p>Note that maximum values are used together so that if any maximum (number of calls, total time
 * taken) is reached, process will end in failures.
 */
@ConfigMapping(
    prefix = "stargate.bridge.bootstrap",
    namingStrategy = ConfigMapping.NamingStrategy.KEBAB_CASE)
public interface BridgeBootstrapConfig {
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
}
