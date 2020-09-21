package io.stargate.health.metrics.api;

import com.codahale.metrics.MetricRegistry;

/**
 * Entry point to the Stargate metrics.
 *
 * <p>This gets registered as a service in the OSGi bundle context. It can be accessed by
 * extensions, either to publish their own metrics, or access existing metrics and expose them.
 */
public interface Metrics {

  /**
   * Returns a global registry that contains the metrics of all extensions.
   *
   * <p>This should only be used to read the metrics. Extensions that wish to publish their own
   * metrics should use {@link #getRegistry(String)} instead, in order to scope their metric paths
   * and avoid collisions.
   */
  MetricRegistry getRegistry();

  /**
   * Returns a view of the {@link #getRegistry() global registry} restricted by the given prefix,
   * which should typically be the extension's name.
   *
   * <p>The view only lists the metrics starting with the prefix. Conversely, the prefix is
   * automatically added when registering new metrics.
   */
  MetricRegistry getRegistry(String prefix);
}
