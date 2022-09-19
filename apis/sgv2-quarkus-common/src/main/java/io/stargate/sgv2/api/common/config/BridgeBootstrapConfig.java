package io.stargate.sgv2.api.common.config;

import io.smallrye.config.ConfigMapping;

/**
 * Configuration value definition for configuring aspects of Bridge Bootstrapping: bootstrapping
 * consists of fetching Persistence System capabilities metadata during the startup of Services.
 */
@ConfigMapping(
    prefix = "stargate.bridge.bootstrap",
    namingStrategy = ConfigMapping.NamingStrategy.KEBAB_CASE)
public interface BridgeBootstrapConfig {
  /**
   * Configuration of retry settings.
   *
   * @return
   */
  RetryableCallsConfig retry();

  /** We also include an actual implementation to use by tests. */
  public record Impl(RetryableCallsConfig retry) implements BridgeBootstrapConfig {
    public Impl() {
      this(new RetryableCallsConfig.Impl());
    }
  }
}
