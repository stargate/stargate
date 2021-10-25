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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * Simple {@link RequestEventListener} that increases a Micrometer counter once the request is
 * finished.
 */
public class CounterRequestEventListener implements RequestEventListener {

  private final MeterRegistry registry;
  private final JerseyTagsProvider tagsProvider;
  private final String metricName;

  /**
   * Default constructor.
   *
   * @param registry {@link MeterRegistry} to report to.
   * @param tagsProvider {@link JerseyTagsProvider} to use for metrics tags based on the event. Note
   *     that only {@link JerseyTagsProvider#httpRequestTags(RequestEvent)} is consulted here, as
   *     the counting is done at the end of the event and this listener has no notion of
   *     long-running tasks.
   * @param metricName Name of the metric to use.
   */
  public CounterRequestEventListener(
      MeterRegistry registry, JerseyTagsProvider tagsProvider, String metricName) {
    this.registry = registry;
    this.tagsProvider = tagsProvider;
    this.metricName = metricName;
  }

  /** {@inheritDoc} */
  @Override
  public void onEvent(RequestEvent event) {
    // only on finish, increase counter by one
    RequestEvent.Type type = event.getType();
    if (type == RequestEvent.Type.FINISHED) {
      Counter counter = registry.counter(metricName, tagsProvider.httpRequestTags(event));
      counter.increment();
    }
  }
}
