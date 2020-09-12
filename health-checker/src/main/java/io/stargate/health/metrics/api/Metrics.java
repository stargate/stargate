package io.stargate.health.metrics.api;

import com.codahale.metrics.MetricRegistry;

/**
 * Entry point to the Stargate metrics.
 *
 * <p>This gets registered as a service in the OSGi bundle context. It can be accessed by
 * extensions, either to publish their own metrics, or access existing metrics and expose them.
 */
public interface Metrics {
  MetricRegistry getRegistry();
}
