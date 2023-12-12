package io.stargate.sgv2.api.common.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.regex.Pattern;

@ApplicationScoped
public class TenantRequestMetricsTagProvider implements HttpServerMetricsTagsContributor {

  // split pattern for the user agent, extract only first part of the agent
  private static final Pattern USER_AGENT_SPLIT = Pattern.compile("[\\s/]");

  // same as V1 io.stargate.core.metrics.StargateMetricConstants#UNKNOWN
  private static final String UNKNOWN_VALUE = "unknown";

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
  public TenantRequestMetricsTagProvider(
      StargateRequestInfo requestInfo, MetricsConfig metricsConfig) {
    this.requestInfo = requestInfo;
    this.config = metricsConfig.tenantRequestCounter();
    errorTrue = Tag.of(config.errorTag(), "true");
    errorFalse = Tag.of(config.errorTag(), "false");
    tenantUnknown = Tag.of(config.tenantTag(), UNKNOWN_VALUE);
  }

  public Tags contribute(Context context) {
    // resolve tenant
    Tag tenantTag =
        requestInfo.getTenantId().map(id -> Tag.of(config.tenantTag(), id)).orElse(tenantUnknown);

    // check if we need user agent as well
    Tags tags = Tags.of(tenantTag);
    if (config.userAgentTagEnabled()) {
      String userAgentValue = getUserAgentValue(context.request());
      tags = tags.and(Tag.of(config.userAgentTag(), userAgentValue));
    }
    return tags;
  }

  private String getUserAgentValue(HttpServerRequest request) {
    String headerString = request.getHeader("user-agent");
    if (null != headerString && !headerString.isBlank()) {
      String[] split = USER_AGENT_SPLIT.split(headerString);
      if (split.length > 0) {
        return split[0];
      } else {
        return headerString;
      }
    } else {
      return UNKNOWN_VALUE;
    }
  }
}
