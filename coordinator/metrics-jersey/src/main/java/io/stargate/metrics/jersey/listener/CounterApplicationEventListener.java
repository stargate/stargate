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

package io.stargate.metrics.jersey.listener;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * The Micrometer {@link ApplicationEventListener} which registers {@link
 * CounterRequestEventListener} for counting Jersey server requests.
 */
public class CounterApplicationEventListener implements ApplicationEventListener {

  private final MeterRegistry meterRegistry;
  private final JerseyTagsProvider tagsProvider;
  private final String metricName;

  /**
   * Default constructor.
   *
   * @param meterRegistry {@link MeterRegistry} to report to.
   * @param tagsProvider {@link JerseyTagsProvider} to use for metrics tags based on the event. Note
   *     that only {@link JerseyTagsProvider#httpRequestTags(RequestEvent)} is consulted here, as
   *     the counting is done at the end of the event and this listener has no notion of
   *     long-running tasks.
   * @param metricName Name of the metric to use.
   */
  public CounterApplicationEventListener(
      MeterRegistry meterRegistry, JerseyTagsProvider tagsProvider, String metricName) {
    this.meterRegistry = meterRegistry;
    this.tagsProvider = tagsProvider;
    this.metricName = metricName;
  }

  /** {@inheritDoc} */
  @Override
  public void onEvent(ApplicationEvent event) {
    // ignored
  }

  /** {@inheritDoc} */
  @Override
  public RequestEventListener onRequest(RequestEvent requestEvent) {
    return new CounterRequestEventListener(meterRegistry, tagsProvider, metricName);
  }
}
