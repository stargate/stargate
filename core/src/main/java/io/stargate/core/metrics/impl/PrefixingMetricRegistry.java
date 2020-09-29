package io.stargate.core.metrics.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

/**
 * Wraps an existing registry to prefix every new metric with a given string on the fly.
 *
 * <p>For clients of this class, it looks like they're interacting with a regular registry, when
 * they are in fact only seeing one part of a bigger registry, that has other metrics under
 * different prefixes.
 *
 * <p>Implementation note: unfortunately {@link MetricRegistry} is a class, not an interface. So we
 * need to extend, even though we delegate everything to another instance.
 */
public class PrefixingMetricRegistry extends MetricRegistry {

  private final MetricRegistry backingRegistry;
  private final String prefix;
  private final String prefixDot;
  private final MetricFilter isPrefixedFilter;

  public PrefixingMetricRegistry(MetricRegistry backingRegistry, String prefix) {
    this.backingRegistry = backingRegistry;
    this.prefix = prefix;
    this.prefixDot = prefix + "."; // precompute this because we use it often
    this.isPrefixedFilter = (name, metric) -> PrefixingMetricRegistry.this.isPrefixed(name);
  }

  @Override
  protected ConcurrentMap<String, Metric> buildMap() {
    // Null out `metrics` in the parent class.
    // Note: We can't do the same for `listeners` because it's not accessible, so we're wasting a
    // list instance.
    return null;
  }

  @Override
  public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
    return backingRegistry.register(addPrefix(name), metric);
  }

  @Override
  public void registerAll(MetricSet metrics) throws IllegalArgumentException {
    backingRegistry.registerAll(prefix, metrics);
  }

  @Override
  public void registerAll(String prefix, MetricSet metrics) throws IllegalArgumentException {
    backingRegistry.registerAll(addPrefix(prefix), metrics);
  }

  @Override
  public Counter counter(String name) {
    return backingRegistry.counter(addPrefix(name));
  }

  @Override
  public Counter counter(String name, MetricSupplier<Counter> supplier) {
    return backingRegistry.counter(addPrefix(name), supplier);
  }

  @Override
  public Histogram histogram(String name) {
    return backingRegistry.histogram(addPrefix(name));
  }

  @Override
  public Histogram histogram(String name, MetricSupplier<Histogram> supplier) {
    return backingRegistry.histogram(addPrefix(name), supplier);
  }

  @Override
  public Meter meter(String name) {
    return backingRegistry.meter(addPrefix(name));
  }

  @Override
  public Meter meter(String name, MetricSupplier<Meter> supplier) {
    return backingRegistry.meter(addPrefix(name), supplier);
  }

  @Override
  public Timer timer(String name) {
    return backingRegistry.timer(addPrefix(name));
  }

  @Override
  public Timer timer(String name, MetricSupplier<Timer> supplier) {
    return backingRegistry.timer(addPrefix(name), supplier);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Gauge gauge(String name, MetricSupplier<Gauge> supplier) {
    return backingRegistry.gauge(addPrefix(name), supplier);
  }

  @Override
  public boolean remove(String name) {
    return backingRegistry.remove(addPrefix(name));
  }

  @Override
  public void removeMatching(MetricFilter filter) {
    backingRegistry.removeMatching(new PrefixingFilter(filter));
  }

  @Override
  public void addListener(MetricRegistryListener listener) {
    backingRegistry.addListener(new PrefixingListener(listener));
  }

  @Override
  public void removeListener(MetricRegistryListener listener) {
    // A bit wasteful to re-wrap just for this, but that way we don't have to keep track of our
    // listeners ourselves.
    // Note that this relies on PrefixingListener implementing `equals` correctly.
    backingRegistry.removeListener(new PrefixingListener(listener));
  }

  @Override
  public SortedSet<String> getNames() {
    return stripPrefix(backingRegistry.getNames());
  }

  @Override
  @SuppressWarnings("rawtypes")
  public SortedMap<String, Gauge> getGauges() {
    return stripPrefix(backingRegistry.getGauges(isPrefixedFilter));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return stripPrefix(backingRegistry.getGauges(new PrefixingFilter(filter)));
  }

  @Override
  public SortedMap<String, Counter> getCounters() {
    return stripPrefix(backingRegistry.getCounters(isPrefixedFilter));
  }

  @Override
  public SortedMap<String, Counter> getCounters(MetricFilter filter) {
    return stripPrefix(backingRegistry.getCounters(new PrefixingFilter(filter)));
  }

  @Override
  public SortedMap<String, Histogram> getHistograms() {
    return stripPrefix(backingRegistry.getHistograms(isPrefixedFilter));
  }

  @Override
  public SortedMap<String, Histogram> getHistograms(MetricFilter filter) {
    return stripPrefix(backingRegistry.getHistograms(new PrefixingFilter(filter)));
  }

  @Override
  public SortedMap<String, Meter> getMeters() {
    return stripPrefix(backingRegistry.getMeters(isPrefixedFilter));
  }

  @Override
  public SortedMap<String, Meter> getMeters(MetricFilter filter) {
    return stripPrefix(backingRegistry.getMeters(new PrefixingFilter(filter)));
  }

  @Override
  public SortedMap<String, Timer> getTimers() {
    return stripPrefix(backingRegistry.getTimers(isPrefixedFilter));
  }

  @Override
  public SortedMap<String, Timer> getTimers(MetricFilter filter) {
    return stripPrefix(backingRegistry.getTimers(new PrefixingFilter(filter)));
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return stripPrefix(backingRegistry.getMetrics());
  }

  private String addPrefix(String name) {
    return MetricRegistry.name(prefix, name);
  }

  private boolean isPrefixed(String name) {
    return name.startsWith(prefixDot);
  }

  private String stripPrefix(String name) {
    assert isPrefixed(name); // callers must check that first
    return name.substring(prefixDot.length());
  }

  private SortedSet<String> stripPrefix(SortedSet<String> in) {
    TreeSet<String> out = new TreeSet<>();
    for (String name : in) {
      if (isPrefixed(name)) {
        out.add(stripPrefix(name));
      }
    }
    return Collections.unmodifiableSortedSet(out);
  }

  private <MetricT extends Metric> SortedMap<String, MetricT> stripPrefix(Map<String, MetricT> in) {
    SortedMap<String, MetricT> out = new TreeMap<>();
    for (Map.Entry<String, MetricT> entry : in.entrySet()) {
      String name = entry.getKey();
      if (isPrefixed(name)) {
        out.put(stripPrefix(name), entry.getValue());
      }
    }
    return Collections.unmodifiableSortedMap(out);
  }

  /**
   * Wraps a prefix-unaware filter to only invoke it for metrics that match our prefix, and strip
   * the prefix before performing the test.
   */
  private class PrefixingFilter implements MetricFilter {
    private final MetricFilter filter;

    public PrefixingFilter(MetricFilter filter) {
      this.filter = filter;
    }

    @Override
    public boolean matches(String name, Metric metric) {
      return isPrefixed(name) && filter.matches(stripPrefix(name), metric);
    }
  }

  /**
   * Wraps a prefix-unaware listener to only invoke it for metrics that match our prefix, and strip
   * the prefix before invoking the callbacks.
   */
  private class PrefixingListener implements MetricRegistryListener {
    private final MetricRegistryListener listener;

    public PrefixingListener(MetricRegistryListener listener) {
      this.listener = listener;
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
      if (isPrefixed(name)) {
        listener.onGaugeAdded(stripPrefix(name), gauge);
      }
    }

    @Override
    public void onGaugeRemoved(String name) {
      if (isPrefixed(name)) {
        listener.onGaugeRemoved(stripPrefix(name));
      }
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
      if (isPrefixed(name)) {
        listener.onCounterAdded(stripPrefix(name), counter);
      }
    }

    @Override
    public void onCounterRemoved(String name) {
      if (isPrefixed(name)) {
        listener.onCounterRemoved(stripPrefix(name));
      }
    }

    @Override
    public void onHistogramAdded(String name, Histogram histogram) {
      if (isPrefixed(name)) {
        listener.onHistogramAdded(stripPrefix(name), histogram);
      }
    }

    @Override
    public void onHistogramRemoved(String name) {
      if (isPrefixed(name)) {
        listener.onHistogramRemoved(stripPrefix(name));
      }
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
      if (isPrefixed(name)) {
        listener.onMeterAdded(stripPrefix(name), meter);
      }
    }

    @Override
    public void onMeterRemoved(String name) {
      if (isPrefixed(name)) {
        listener.onMeterRemoved(stripPrefix(name));
      }
    }

    @Override
    public void onTimerAdded(String name, Timer timer) {
      if (isPrefixed(name)) {
        listener.onTimerAdded(stripPrefix(name), timer);
      }
    }

    @Override
    public void onTimerRemoved(String name) {
      if (isPrefixed(name)) {
        listener.onTimerRemoved(stripPrefix(name));
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof PrefixingListener) {
        PrefixingListener that = (PrefixingListener) other;
        return Objects.equals(this.listener, that.listener);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return listener.hashCode();
    }
  }
}
