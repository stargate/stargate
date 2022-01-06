/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.SortedMap;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxCounterMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxHistogramMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxMeterMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxTimerMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.jupiter.api.Test;

public class Cassandra311MetricsRegistryTest {

  @Test
  public void versionCheck() {
    String supportedVersion = "3.11.11";
    String currentVersion = FBUtilities.getReleaseVersionString();

    assertThat(currentVersion)
        .withFailMessage(
            "The supported Cassandra version has changed, please confirm that there were no changes to org.apache.cassandra.metrics.CassandraMetricsRegistry")
        .isEqualTo(supportedVersion);
  }

  // A class with a name ending in '$'
  private static class StrangeName$ {}

  @Test
  public void testChooseType() {
    assertThat(MetricName.chooseType(null, StrangeName$.class)).isEqualTo("StrangeName");
    assertThat(MetricName.chooseType("", StrangeName$.class)).isEqualTo("StrangeName");
    assertThat(MetricName.chooseType(null, String.class)).isEqualTo("String");
    assertThat(MetricName.chooseType("", String.class)).isEqualTo("String");

    assertThat(MetricName.chooseType("a", StrangeName$.class)).isEqualTo("a");
    assertThat(MetricName.chooseType("b", String.class)).isEqualTo("b");
  }

  @Test
  public void testMetricName() {
    MetricName name = new MetricName(StrangeName$.class, "NaMe", "ScOpE");
    assertThat(name.getType()).isEqualTo("StrangeName");
  }

  @Test
  public void testJvmMetricsRegistration() {
    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;

    // Same registration as CassandraDaemon
    registry.register(
        "jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    registry.register("jvm.gc", new GarbageCollectorMetricSet());
    registry.register("jvm.memory", new MemoryUsageGaugeSet());

    Collection<String> names = registry.getNames();

    // No metric with ".." in name
    assertThat(names.stream().noneMatch(name -> name.contains(".."))).isTrue();

    // There should be several metrics within each category
    for (String category : new String[] {"jvm.buffers", "jvm.gc", "jvm.memory"}) {
      assertThat(names.stream().filter(name -> name.startsWith(category + '.')).count() > 1)
          .isTrue();
    }
  }

  @Test
  public void testJvmMetricsRegistrationActual() {
    MetricRegistry registry = CassandraMetricsRegistry.actualRegistry;

    // Same registration as CassandraDaemon
    registry.register(
        "jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    registry.register("jvm.gc", new GarbageCollectorMetricSet());
    registry.register("jvm.memory", new MemoryUsageGaugeSet());

    Collection<String> names = registry.getNames();

    // No metric with ".." in name
    assertThat(names.stream().noneMatch(name -> name.contains(".."))).isTrue();

    // There should be several metrics within each category
    for (String category : new String[] {"jvm.buffers", "jvm.gc", "jvm.memory"}) {
      assertThat(names.stream().filter(name -> name.startsWith(category + '.')).count() > 1)
          .isTrue();
    }
  }

  @Test
  public void testRegisterAndRemove() throws MalformedObjectNameException {
    String group = "foo.counter.registerAndRemove";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    Counter counter = registry.counter(metricName);
    registry.register(metricName, counter);

    counter.inc();

    SortedMap<String, Counter> counters = actualRegistry.getCounters();
    Counter metric = counters.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxCounterMBean mBean =
        JMX.newMBeanProxy(
            server, new ObjectName(createMBeanName(group, type, name)), JmxCounterMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    registry.remove(metricName);
    assertThat(actualRegistry.getCounters().containsKey(metricName.getMetricName())).isFalse();
    assertThat(server.isRegistered(new ObjectName(createMBeanName(group, type, name)))).isFalse();
  }

  @Test
  public void testCounter() throws MalformedObjectNameException {
    String group = "foo.counter";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    Counter counter = registry.counter(metricName);
    registry.register(metricName, counter);

    counter.inc();

    SortedMap<String, Counter> counters = actualRegistry.getCounters();
    Counter metric = counters.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxCounterMBean mBean =
        JMX.newMBeanProxy(
            server, new ObjectName(createMBeanName(group, type, name)), JmxCounterMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    counter.inc();

    assertThat(metric.getCount()).isEqualTo(2);
    assertThat(mBean.getCount()).isEqualTo(2);
  }

  @Test
  public void testCounterAlias() throws MalformedObjectNameException {
    String group = "foo.counter.alias";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    MetricName aliasMetricName = new MetricName(group, type, name + "Alias");
    Counter counter = registry.counter(metricName, aliasMetricName);
    registry.register(metricName, aliasMetricName, counter);

    counter.inc();

    SortedMap<String, Counter> counters = actualRegistry.getCounters();
    Counter metric = counters.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxCounterMBean mBean =
        JMX.newMBeanProxy(
            server,
            new ObjectName(createMBeanName(group, type, name + "Alias")),
            JmxCounterMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    counter.inc();

    assertThat(metric.getCount()).isEqualTo(2);
    assertThat(mBean.getCount()).isEqualTo(2);
  }

  @Test
  public void testMeter() throws MalformedObjectNameException {
    String group = "foo.meter";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    Meter meter = registry.meter(metricName);
    registry.register(metricName, meter);

    meter.mark();

    SortedMap<String, Meter> meters = actualRegistry.getMeters();
    Meter metric = meters.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxMeterMBean mBean =
        JMX.newMBeanProxy(
            server, new ObjectName(createMBeanName(group, type, name)), JmxMeterMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    meter.mark();

    assertThat(metric.getCount()).isEqualTo(2);
    assertThat(mBean.getCount()).isEqualTo(2);
  }

  @Test
  public void testMeterAlias() throws MalformedObjectNameException {
    String group = "foo.meter.alias";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    MetricName aliasMetricName = new MetricName(group, type, name + "Alias");
    Meter meter = registry.meter(metricName, aliasMetricName);
    registry.register(metricName, aliasMetricName, meter);

    meter.mark();

    SortedMap<String, Meter> meters = actualRegistry.getMeters();
    Meter metric = meters.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxMeterMBean mBean =
        JMX.newMBeanProxy(
            server,
            new ObjectName(createMBeanName(group, type, name + "Alias")),
            JmxMeterMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    meter.mark();

    assertThat(metric.getCount()).isEqualTo(2);
    assertThat(mBean.getCount()).isEqualTo(2);
  }

  @Test
  public void testHistogram() throws MalformedObjectNameException {
    String group = "foo.histogram";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    Histogram histogram = registry.histogram(metricName, false);
    registry.register(metricName, histogram);

    histogram.update(1);

    SortedMap<String, Histogram> histograms = actualRegistry.getHistograms();
    Histogram metric = histograms.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxHistogramMBean mBean =
        JMX.newMBeanProxy(
            server, new ObjectName(createMBeanName(group, type, name)), JmxHistogramMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    histogram.update(2);

    assertThat(metric.getCount()).isEqualTo(2);
    assertThat(mBean.getCount()).isEqualTo(2);
  }

  @Test
  public void testHistogramAlias() throws MalformedObjectNameException {
    String group = "foo.histogram.alias";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    MetricName aliasMetricName = new MetricName(group, type, name + "Alias");
    Histogram histogram = registry.histogram(metricName, aliasMetricName, false);
    registry.register(metricName, aliasMetricName, histogram);

    histogram.update(1);

    SortedMap<String, Histogram> histograms = actualRegistry.getHistograms();
    Histogram metric = histograms.get(metricName.getMetricName());
    assertThat(metric.getCount()).isEqualTo(1);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxHistogramMBean mBean =
        JMX.newMBeanProxy(
            server,
            new ObjectName(createMBeanName(group, type, name + "Alias")),
            JmxHistogramMBean.class);

    assertThat(mBean.getCount()).isEqualTo(1);

    histogram.update(2);

    assertThat(metric.getCount()).isEqualTo(2);
    assertThat(mBean.getCount()).isEqualTo(2);
  }

  @Test
  public void testTimer() throws MalformedObjectNameException {
    String group = "foo.timer";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    Timer timer = registry.timer(metricName);
    registry.register(metricName, timer);

    timer.update(Duration.ofNanos(5));

    SortedMap<String, Timer> timers = actualRegistry.getTimers();
    Timer metric = timers.get(metricName.getMetricName());
    assertThat(metric.getSnapshot().getMax()).isEqualTo(5);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxTimerMBean mBean =
        JMX.newMBeanProxy(
            server, new ObjectName(createMBeanName(group, type, name)), JmxTimerMBean.class);

    assertThat(mBean.getMax()).isEqualTo(0.005);

    timer.update(Duration.ofNanos(10));

    assertThat(metric.getSnapshot().getMax()).isEqualTo(10);
    assertThat(mBean.getMax()).isEqualTo(0.01);
  }

  @Test
  public void testTimerAlias() throws MalformedObjectNameException {
    String group = "foo.timer.alias";
    String type = "bar";
    String name = "buzz";

    CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
    MetricRegistry actualRegistry = CassandraMetricsRegistry.actualRegistry;

    MetricName metricName = new MetricName(group, type, name);
    MetricName aliasMetricName = new MetricName(group, type, name + "Alias");
    Timer timer = registry.timer(metricName, aliasMetricName);
    registry.register(metricName, aliasMetricName, timer);

    timer.update(Duration.ofNanos(5));

    SortedMap<String, Timer> timers = actualRegistry.getTimers();
    Timer metric = timers.get(metricName.getMetricName());
    assertThat(metric.getSnapshot().getMax()).isEqualTo(5);

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    JmxTimerMBean mBean =
        JMX.newMBeanProxy(
            server,
            new ObjectName(createMBeanName(group, type, name + "Alias")),
            JmxTimerMBean.class);

    assertThat(mBean.getMax()).isEqualTo(0.005);

    timer.update(Duration.ofNanos(10));

    assertThat(metric.getSnapshot().getMax()).isEqualTo(10);
    assertThat(mBean.getMax()).isEqualTo(0.01);
  }

  private String createMBeanName(String group, String type, String name) {
    final StringBuilder nameBuilder = new StringBuilder();
    nameBuilder.append(ObjectName.quote(group));
    nameBuilder.append(":type=");
    nameBuilder.append(ObjectName.quote(type));
    if (name.length() > 0) {
      nameBuilder.append(",name=");
      nameBuilder.append(ObjectName.quote(name));
    }
    return nameBuilder.toString();
  }
}
