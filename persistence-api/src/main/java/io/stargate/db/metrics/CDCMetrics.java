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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stargate.db.metrics;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

public final class CDCMetrics {
  public static final CDCMetrics instance = new CDCMetrics();

  private static final MetricNameFactory factory = new DefaultNameFactory("CDC");

  private boolean initialized = false;
  private MetricRegistry metricRegistry;

  private Counter producerMessagesInFlight;
  private Meter producerFailures;
  private Meter producerTimedOut;
  private Timer producerLatency;

  private CDCMetrics() {}

  public void markProducerTimedOut() {
    producerTimedOut.mark();
  }

  public void markProducerFailure() {
    producerFailures.mark();
  }

  public void incrementInFlight() {
    producerMessagesInFlight.inc();
  }

  public void decrementInFlight() {
    producerMessagesInFlight.dec();
  }

  public void updateLatency(long nanos) {
    producerLatency.update(nanos, NANOSECONDS);
  }

  public synchronized void init(MetricRegistry metricRegistry) {
    if (initialized) return;

    this.metricRegistry = metricRegistry;

    producerMessagesInFlight = registerCounter("ProducerMessagesInFlight");
    producerFailures = registerMeter("ProducerFailures");
    producerTimedOut = registerMeter("ProducerTimedOut");
    producerLatency = registerTimer("ProducerLatency");

    initialized = true;
  }

  private Meter registerMeter(String name) {
    return metricRegistry.meter(factory.createMetricName(name).getMetricName());
  }

  private Counter registerCounter(String name) {
    return metricRegistry.counter(factory.createMetricName(name).getMetricName());
  }

  private Timer registerTimer(String name) {
    return metricRegistry.timer(factory.createMetricName(name).getMetricName());
  }
}
