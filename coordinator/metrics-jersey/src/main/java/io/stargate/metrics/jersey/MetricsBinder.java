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

package io.stargate.metrics.jersey;

import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.micrometer.jersey2.server.MetricsApplicationEventListener;
import io.stargate.core.metrics.StargateMetricConstants;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.metrics.jersey.config.MetricsListenerConfig;
import io.stargate.metrics.jersey.config.SystemPropsMetricsListenerConfig;
import io.stargate.metrics.jersey.listener.CounterApplicationEventListener;
import io.stargate.metrics.jersey.tags.CompositeJerseyTagsProvider;
import io.stargate.metrics.jersey.tags.ConstantTagsProvider;
import io.stargate.metrics.jersey.tags.DocsApiModuleTagsProvider;
import io.stargate.metrics.jersey.tags.HeadersTagProvider;
import io.stargate.metrics.jersey.tags.HttpCounterTagsProvider;
import io.stargate.metrics.jersey.tags.HttpMeterTagsProvider;
import io.stargate.metrics.jersey.tags.NonApiModuleTagsProvider;
import io.stargate.metrics.jersey.tags.PathParametersTagsProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Class that registers all the needed metric application listeners to the Jersey environment for a
 * module.
 *
 * <p>By default it registers the:
 *
 * <ol>
 *   <li>Meter listener, that meters HTTP requests
 *   <li>Counter listener, that counts HTTP requests
 * </ol>
 *
 * Both listeners can be disabled by setting <code>stargate.metrics.http_meter_listener.enabled
 * </code> or <code>stargate.metrics.http_counter_listener.enabled</code> to false.
 *
 * <p>Both listeners use default tag providers, module name and path parameters (if enabled). The
 * meter listener adds {@link HttpMeterTagsProvider} in addition, that adds method, URI and status
 * to the tags. The counter listener adds {@link HttpCounterTagsProvider} in addition, that adds
 * only the exception tag.
 *
 * <p>Both of those providers extend on top of the {@link HttpMetricsTagProvider} that is passed as
 * the constructor parameter to this class. However, each provider can ignore the global {@link
 * HttpMetricsTagProvider} by setting <code>
 * stargate.metrics.http_meter_listener.ignore_http_tags_provider</code> or <code>
 * stargate.metrics.http_counter_listener.ignore_http_tags_provider</code> to true.
 */
public class MetricsBinder {

  private final Metrics metrics;
  private final HttpMetricsTagProvider httpMetricsTagProvider;
  private final String module;
  private final Collection<String> nonApiUriRegexes;
  private final MetricsListenerConfig meterListenerConfig;
  private final MetricsListenerConfig counterListenerConfig;

  /**
   * Default constructor with no non-APR URI regexes. Uses {@link SystemPropsMetricsListenerConfig}
   * for metering and counting configuration.
   *
   * @param metrics {@link Metrics} instance.
   * @param httpMetricsTagProvider Global {@link HttpMetricsTagProvider} registered in the OSGi
   * @param module Module name
   */
  public MetricsBinder(
      Metrics metrics, HttpMetricsTagProvider httpMetricsTagProvider, String module) {
    this(metrics, httpMetricsTagProvider, module, Collections.emptyList());
  }

  /**
   * Default constructor. Uses {@link SystemPropsMetricsListenerConfig} for metering and counting
   * configuration.
   *
   * @param metrics {@link Metrics} instance.
   * @param httpMetricsTagProvider Global {@link HttpMetricsTagProvider} registered in the OSGi
   * @param module Module name
   * @param nonApiUriRegexes List of regexes for URIs that should be tagged with #module-other tag.
   */
  public MetricsBinder(
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String module,
      Collection<String> nonApiUriRegexes) {
    this(
        metrics,
        httpMetricsTagProvider,
        module,
        nonApiUriRegexes,
        new SystemPropsMetricsListenerConfig("stargate.metrics.http_meter_listener"),
        new SystemPropsMetricsListenerConfig("stargate.metrics.http_counter_listener"));
  }

  /**
   * Secondary constructor.
   *
   * @param metrics {@link Metrics} instance.
   * @param httpMetricsTagProvider Global {@link HttpMetricsTagProvider} registered in the OSGi
   * @param module Module name
   * @param nonApiUriRegexes List of regexes for URIs that should be tagged with #module-other tag.
   * @param meterListenerConfig config for metering HTTP requests
   * @param counterListenerConfig config for counting HTTP requests
   */
  public MetricsBinder(
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String module,
      Collection<String> nonApiUriRegexes,
      MetricsListenerConfig meterListenerConfig,
      MetricsListenerConfig counterListenerConfig) {
    this.metrics = metrics;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.module = module;
    this.nonApiUriRegexes = nonApiUriRegexes;
    this.meterListenerConfig = meterListenerConfig;
    this.counterListenerConfig = counterListenerConfig;
  }

  /**
   * Registers application listeners in the {@link JerseyEnvironment}, based on the {@link
   * #meterListenerConfig} and {@link #counterListenerConfig}.
   *
   * @param jersey {@link JerseyEnvironment}
   */
  public void register(JerseyEnvironment jersey) {
    if (meterListenerConfig.isEnabled()) {
      JerseyTagsProvider meterTagsProvider =
          getMeterTagsProvider(
              meterListenerConfig, metrics, httpMetricsTagProvider, module, nonApiUriRegexes);
      MetricsApplicationEventListener listener =
          new MetricsApplicationEventListener(
              metrics.getMeterRegistry(),
              meterTagsProvider,
              StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS,
              true);
      jersey.register(listener);
    }

    if (counterListenerConfig.isEnabled()) {
      JerseyTagsProvider counterTagsProvider =
          getCounterTagsProvider(
              counterListenerConfig, metrics, httpMetricsTagProvider, module, nonApiUriRegexes);
      CounterApplicationEventListener listener =
          new CounterApplicationEventListener(
              metrics.getMeterRegistry(),
              counterTagsProvider,
              StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS_COUNTER);
      jersey.register(listener);
    }
  }

  private static JerseyTagsProvider getMeterTagsProvider(
      MetricsListenerConfig config,
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String module,
      Collection<String> nonApiUriRegexes) {
    // resolve if http tag provider should be ignored or not
    HttpMeterTagsProvider resourceProvider =
        config.isIgnoreHttpMetricProvider()
            ? new HttpMeterTagsProvider()
            : new HttpMeterTagsProvider(httpMetricsTagProvider);

    // get default tags and add the meter provider
    List<JerseyTagsProvider> allProviders =
        new ArrayList<>(getDefaultTagsProvider(metrics, module, nonApiUriRegexes));
    allProviders.add(resourceProvider);

    // return composite containing all the providers
    return new CompositeJerseyTagsProvider(allProviders);
  }

  private static JerseyTagsProvider getCounterTagsProvider(
      MetricsListenerConfig config,
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      String module,
      Collection<String> nonApiUriRegexes) {
    // resolve if http tag provider should be ignored or not
    HttpCounterTagsProvider resourceProvider =
        config.isIgnoreHttpMetricProvider()
            ? new HttpCounterTagsProvider()
            : new HttpCounterTagsProvider(httpMetricsTagProvider);

    // get default tags and add the meter provider
    List<JerseyTagsProvider> allProviders =
        new ArrayList<>(getDefaultTagsProvider(metrics, module, nonApiUriRegexes));
    allProviders.add(resourceProvider);

    // return composite containing all the providers
    return new CompositeJerseyTagsProvider(allProviders);
  }

  private static List<JerseyTagsProvider> getDefaultTagsProvider(
      Metrics metrics, String module, Collection<String> nonApiUriRegexes) {
    ConstantTagsProvider defaultProvider = new ConstantTagsProvider(metrics.tagsForModule(module));
    PathParametersTagsProvider pathParametersProvider = new PathParametersTagsProvider();
    HeadersTagProvider headersTagProvider = new HeadersTagProvider();
    NonApiModuleTagsProvider nonApiModuleTagsProvider =
        new NonApiModuleTagsProvider(metrics, module, nonApiUriRegexes);
    DocsApiModuleTagsProvider docsApiProvider = new DocsApiModuleTagsProvider(metrics);

    return Arrays.asList(
        defaultProvider,
        pathParametersProvider,
        headersTagProvider,
        nonApiModuleTagsProvider,
        docsApiProvider);
  }
}
