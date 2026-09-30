/*
 * Copyright 2017 VMware, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.jersey2.server;

import static java.util.Objects.requireNonNull;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * {@link RequestEventListener} recording timings for Jersey server requests.
 *
 * <p>Vendored from micrometer-jersey2 1.8.x to avoid the removed artifact dependency.
 */
public class MetricsRequestEventListener implements RequestEventListener {

  private final Map<ContainerRequest, Timer.Sample> shortTaskSample =
      Collections.synchronizedMap(new IdentityHashMap<>());

  private final Map<ContainerRequest, Collection<LongTaskTimer.Sample>> longTaskSamples =
      Collections.synchronizedMap(new IdentityHashMap<>());

  private final Map<ContainerRequest, Set<Timed>> timedAnnotationsOnRequest =
      Collections.synchronizedMap(new IdentityHashMap<>());

  private final MeterRegistry registry;

  private final JerseyTagsProvider tagsProvider;

  private final boolean autoTimeRequests;

  private final TimedFinder timedFinder;

  private final String metricName;

  public MetricsRequestEventListener(
      MeterRegistry registry,
      JerseyTagsProvider tagsProvider,
      String metricName,
      boolean autoTimeRequests,
      AnnotationFinder annotationFinder) {
    this.registry = requireNonNull(registry);
    this.tagsProvider = requireNonNull(tagsProvider);
    this.metricName = requireNonNull(metricName);
    this.autoTimeRequests = autoTimeRequests;
    this.timedFinder = new TimedFinder(annotationFinder);
  }

  @Override
  public void onEvent(RequestEvent event) {
    ContainerRequest containerRequest = event.getContainerRequest();
    Set<Timed> timedAnnotations;

    switch (event.getType()) {
      case ON_EXCEPTION:
        if (!isNotFoundException(event)) {
          break;
        }
        // fall through
      case REQUEST_MATCHED:
        timedAnnotations = annotations(event);
        timedAnnotationsOnRequest.put(containerRequest, timedAnnotations);
        shortTaskSample.put(containerRequest, Timer.start(registry));

        List<LongTaskTimer.Sample> longSamples =
            longTaskTimers(timedAnnotations, event).stream()
                .map(LongTaskTimer::start)
                .collect(Collectors.toList());
        if (!longSamples.isEmpty()) {
          this.longTaskSamples.put(containerRequest, longSamples);
        }
        break;

      case FINISHED:
        timedAnnotations = timedAnnotationsOnRequest.remove(containerRequest);
        Timer.Sample shortSample = shortTaskSample.remove(containerRequest);

        if (shortSample != null) {
          for (Timer timer : shortTimers(timedAnnotations, event)) {
            shortSample.stop(timer);
          }
        }

        Collection<LongTaskTimer.Sample> longTaskSampleCollection =
            this.longTaskSamples.remove(containerRequest);
        if (longTaskSampleCollection != null) {
          for (LongTaskTimer.Sample longSample : longTaskSampleCollection) {
            longSample.stop();
          }
        }
        break;

      default:
        break;
    }
  }

  private boolean isNotFoundException(RequestEvent event) {
    Throwable t = event.getException();
    if (t == null) {
      return false;
    }
    String className = t.getClass().getCanonicalName();
    return className.equals("jakarta.ws.rs.NotFoundException")
        || className.equals("javax.ws.rs.NotFoundException");
  }

  private Set<Timer> shortTimers(Set<Timed> timed, RequestEvent event) {
    if ((timed == null || timed.isEmpty()) && autoTimeRequests) {
      return Collections.singleton(registry.timer(metricName, tagsProvider.httpRequestTags(event)));
    }
    if (timed == null) {
      return Collections.emptySet();
    }
    return timed.stream()
        .filter(annotation -> !annotation.longTask())
        .map(
            t ->
                Timer.builder(t, metricName)
                    .tags(tagsProvider.httpRequestTags(event))
                    .register(registry))
        .collect(Collectors.toSet());
  }

  private Set<LongTaskTimer> longTaskTimers(Set<Timed> timed, RequestEvent event) {
    return timed.stream()
        .filter(Timed::longTask)
        .map(LongTaskTimer::builder)
        .map(b -> b.tags(tagsProvider.httpLongRequestTags(event)).register(registry))
        .collect(Collectors.toSet());
  }

  private Set<Timed> annotations(RequestEvent event) {
    final Set<Timed> timed = new HashSet<>();
    final ResourceMethod matchingResourceMethod = event.getUriInfo().getMatchedResourceMethod();
    if (matchingResourceMethod != null) {
      timed.addAll(
          timedFinder.findTimedAnnotations(
              matchingResourceMethod.getInvocable().getHandlingMethod()));
      if (timed.isEmpty()) {
        timed.addAll(
            timedFinder.findTimedAnnotations(
                matchingResourceMethod.getInvocable().getHandlingMethod().getDeclaringClass()));
      }
    }
    return timed;
  }
}
