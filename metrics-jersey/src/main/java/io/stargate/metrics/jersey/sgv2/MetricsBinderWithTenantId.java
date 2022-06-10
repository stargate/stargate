package io.stargate.metrics.jersey.sgv2;

import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.metrics.jersey.MetricsBinder;
import io.stargate.metrics.jersey.config.MetricsListenerConfig;
import java.util.Collection;
import java.util.List;

/**
 * Extension of {@link MetricsBinder} which adds Tenant Id tag(s) extracted using {@link
 * TenantIdFromHostHeaderTagsProvider}.
 */
public class MetricsBinderWithTenantId extends MetricsBinder {
  public MetricsBinderWithTenantId(
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String moduleName,
      Collection<String> nonApiUriRegexes) {
    super(metrics, httpMetricsTagProvider, moduleName, nonApiUriRegexes);
  }

  @Override
  protected List<JerseyTagsProvider> getMeterTagsProviders(
      MetricsListenerConfig config,
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String module,
      Collection<String> nonApiUriRegexes) {
    List<JerseyTagsProvider> providers =
        super.getMeterTagsProviders(
            config, metrics, httpMetricsTagProvider, module, nonApiUriRegexes);
    providers.add(new TenantIdFromHostHeaderTagsProvider());
    return providers;
  }

  @Override
  protected List<JerseyTagsProvider> getCounterTagsProviders(
      MetricsListenerConfig config,
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String module,
      Collection<String> nonApiUriRegexes) {
    List<JerseyTagsProvider> providers =
        super.getCounterTagsProviders(
            config, metrics, httpMetricsTagProvider, module, nonApiUriRegexes);
    providers.add(new TenantIdFromHostHeaderTagsProvider());
    return providers;
  }
}
