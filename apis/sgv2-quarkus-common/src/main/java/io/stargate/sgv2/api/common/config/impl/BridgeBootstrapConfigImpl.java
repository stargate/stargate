package io.stargate.sgv2.api.common.config.impl;

import io.stargate.sgv2.api.common.config.BridgeBootstrapConfig;
import java.time.Duration;

public record BridgeBootstrapConfigImpl(
    int maxCalls, Duration maxTime, Duration initialDelay, Duration maxDelay, double delayRatio)
    implements BridgeBootstrapConfig {
  public BridgeBootstrapConfigImpl() {
    this(
        Integer.parseInt(DEFAULT_MAX_CALLS),
        Duration.parse(DEFAULT_MAX_TIME),
        Duration.parse(DEFAULT_INITIAL_DELAY),
        Duration.parse(DEFAULT_MAX_DELAY),
        Double.parseDouble(DEFAULT_DELAY_RATIO));
  }
}
