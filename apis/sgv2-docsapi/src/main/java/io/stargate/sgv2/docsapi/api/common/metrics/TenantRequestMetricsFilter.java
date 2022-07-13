/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.config.MetricsConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerResponseContext;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;

/**
 * The filter for counting HTTP requests per tenant. Controlled by {@link
 * MetricsConfig.TenantRequestCounterConfig}.
 */
@ApplicationScoped
public class TenantRequestMetricsFilter {

  /** The {@link MeterRegistry} to report to. */
  private final MeterRegistry meterRegistry;

  /** The configuration for metrics. */
  private final MetricsConfig.TenantRequestCounterConfig config;

  /** The request info bean. */
  private final StargateRequestInfo requestInfo;

  /** The tag for error being true, created only once. */
  private final Tag errorTrue;

  /** The tag for error being false, created only once. */
  private final Tag errorFalse;

  /** The tag for tenant being unknown, created only once. */
  Tag tenantUnknown;

  /** Default constructor. */
  @Inject
  public TenantRequestMetricsFilter(
      MeterRegistry meterRegistry, StargateRequestInfo requestInfo, MetricsConfig metricsConfig) {
    this.meterRegistry = meterRegistry;
    this.requestInfo = requestInfo;
    this.config = metricsConfig.tenantRequestCounter();
    errorTrue = Tag.of(config.errorTag(), "true");
    errorFalse = Tag.of(config.errorTag(), "false");
    tenantUnknown = Tag.of(config.tenantTag(), "UNKNOWN");
  }

  /**
   * Filter that this bean produces.
   *
   * @param context ContainerResponseContext
   * @see https://quarkus.io/guides/resteasy-reactive#request-or-response-filters
   */
  @ServerResponseFilter
  public void record(ContainerResponseContext context) {
    // only if enabled
    if (config.enabled()) {

      // resolve tenant
      Tag tenantTag =
          requestInfo.getTenantId().map(id -> Tag.of(config.tenantTag(), id)).orElse(tenantUnknown);

      // resolve error
      boolean error = context.getStatus() >= 500;
      Tag errorTag = error ? errorTrue : errorFalse;

      // record
      meterRegistry.counter(config.metricName(), Tags.of(tenantTag, errorTag)).increment();
    }
  }
}
