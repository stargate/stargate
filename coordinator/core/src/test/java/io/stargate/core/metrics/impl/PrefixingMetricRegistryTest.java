package io.stargate.core.metrics.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistry.MetricSupplier;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PrefixingMetricRegistryTest {

  private static final String PREFIX = "test";

  private MetricRegistry backingRegistry;
  private PrefixingMetricRegistry prefixingRegistry;

  @BeforeEach
  public void setup() {
    backingRegistry = new MetricRegistry();
    prefixingRegistry = new PrefixingMetricRegistry(backingRegistry, PREFIX);
  }

  @Test
  public void should_register_one() {
    // Given
    Counter counter = mock(Counter.class);
    String counterName = "path.to.counter";

    // When
    prefixingRegistry.register(counterName, counter);

    // Then
    assertThat(backingRegistry.getNames()).containsOnly(addPrefix(counterName));
    assertThat(backingRegistry.counter(addPrefix(counterName))).isEqualTo(counter);
  }

  @Test
  public void should_register_all() {
    // Given
    Counter counter = mock(Counter.class);
    String counterName = "path.to.counter";
    Meter meter = mock(Meter.class);
    String meterName = "path.to.meter";
    MetricRegistry metricSet = new MetricRegistry();
    metricSet.register(counterName, counter);
    metricSet.register(meterName, meter);

    // When
    prefixingRegistry.registerAll(metricSet);

    // Then
    assertThat(backingRegistry.getNames())
        .containsOnly(addPrefix(counterName), addPrefix(meterName));
    assertThat(backingRegistry.counter(addPrefix(counterName))).isEqualTo(counter);
    assertThat(backingRegistry.meter(addPrefix(meterName))).isEqualTo(meter);
  }

  @Test
  public void should_register_all_with_prefix() {
    // Given
    Counter counter = mock(Counter.class);
    String counterName = "path.to.counter";
    Meter meter = mock(Meter.class);
    String meterName = "path.to.meter";
    MetricRegistry metricSet = new MetricRegistry();
    metricSet.register(counterName, counter);
    metricSet.register(meterName, meter);

    // When
    prefixingRegistry.registerAll("more", metricSet);

    // Then
    assertThat(backingRegistry.getNames())
        .containsOnly(addPrefix("more." + counterName), addPrefix("more." + meterName));
    assertThat(backingRegistry.counter(addPrefix("more." + counterName))).isEqualTo(counter);
    assertThat(backingRegistry.meter(addPrefix("more." + meterName))).isEqualTo(meter);
  }

  /**
   * Covers all the methods that get or create a specific metric type given a name, such as {@link
   * MetricRegistry#counter(String)}.
   */
  @ParameterizedTest
  @MethodSource("getOrCreateMethods")
  public void should_get_or_create_metric(
      BiFunction<MetricRegistry, String, ? extends Metric> testedMethod) {
    // Given
    String name = "path.to.metric";

    // When
    Metric metric = testedMethod.apply(prefixingRegistry, name);

    // Then
    assertThat(backingRegistry.getNames()).containsOnly(addPrefix(name));
    assertThat(testedMethod.apply(backingRegistry, addPrefix(name))).isEqualTo(metric);

    // When
    Metric metric2 = testedMethod.apply(prefixingRegistry, name);
    assertThat(metric2).isSameAs(metric);
  }

  public static Stream<BiFunction<MetricRegistry, String, ? extends Metric>> getOrCreateMethods() {
    return Stream.of(
        MetricRegistry::counter,
        MetricRegistry::histogram,
        MetricRegistry::meter,
        MetricRegistry::timer);
  }

  /**
   * Covers all the methods that get or create a specific metric type given a name and a supplier,
   * such as {@link MetricRegistry#counter(String, MetricSupplier)}.
   */
  @ParameterizedTest
  @MethodSource("getOrSupplyMethods")
  public <MetricT extends Metric> void should_get_or_supply_metric(
      GetOrSupplyMethod<MetricT> testedMethod, Class<MetricT> metricClass) {

    // Given
    String name = "path.to.metric";
    MetricSupplier<MetricT> supplier = () -> mock(metricClass);

    // When
    MetricT metric = testedMethod.apply(prefixingRegistry, name, supplier);

    // Then
    assertThat(backingRegistry.getNames()).containsOnly(addPrefix(name));
    assertThat(testedMethod.apply(backingRegistry, addPrefix(name), supplier)).isEqualTo(metric);

    // When
    MetricT metric2 = testedMethod.apply(prefixingRegistry, name, supplier);
    assertThat(metric2).isSameAs(metric);
  }

  @SuppressWarnings("rawtypes")
  public static Stream<Arguments> getOrSupplyMethods() {
    return Stream.of(
        arguments((GetOrSupplyMethod<Counter>) MetricRegistry::counter, Counter.class),
        arguments((GetOrSupplyMethod<Histogram>) MetricRegistry::histogram, Histogram.class),
        arguments((GetOrSupplyMethod<Meter>) MetricRegistry::meter, Meter.class),
        arguments((GetOrSupplyMethod<Timer>) MetricRegistry::timer, Timer.class),
        arguments((GetOrSupplyMethod<Gauge>) MetricRegistry::gauge, Gauge.class));
  }

  @Test
  public void should_remove_by_name() {
    // Given
    Counter counter = mock(Counter.class);
    String counterName = "path.to.counter";
    prefixingRegistry.register(counterName, counter);

    // When
    prefixingRegistry.remove(counterName);

    // Then
    assertThat(prefixingRegistry.getNames()).doesNotContain(counterName);
    assertThat(backingRegistry.getNames()).doesNotContain(addPrefix(counterName));
  }

  @Test
  public void should_remove_by_filter() {
    // Given
    String counterName = "path.to.counter";
    prefixingRegistry.counter(counterName);
    String meterName = "path.to.meter";
    prefixingRegistry.meter(meterName);
    // Also register a metric directly in the backing registry, to check that we don't operate
    // outside of our prefix.
    String otherPrefixCounterName = "otherPrefix.path.to.counter";
    backingRegistry.counter(otherPrefixCounterName);

    // When
    prefixingRegistry.removeMatching((name, metric) -> name.contains("counter"));

    // Then
    assertThat(prefixingRegistry.getNames()).containsOnly(meterName);
    assertThat(backingRegistry.getNames())
        .containsOnly(addPrefix(meterName), otherPrefixCounterName);
  }

  @ParameterizedTest
  @MethodSource("listenerMethods")
  public <MetricT extends Metric> void should_notify_listener(
      GetOrSupplyMethod<MetricT> createMethod,
      ListenerOnAddMethod<MetricT> onAddMethod,
      ListenerOnRemoveMethod onRemoveMethod,
      Class<MetricT> metricClass) {

    // Given
    String metricName = "path.to.metric";
    // We'll also maniuplate a metric directly in the backing registry, to check that the listener
    // isn't called back for events outside of our prefix.
    String otherMetricName = "otherPrefix.path.to.metric";
    MetricRegistryListener listener = mock(MetricRegistryListener.class);
    prefixingRegistry.addListener(listener);

    // When creating a metric in the prefix
    MetricT metric = mock(metricClass);
    createMethod.apply(prefixingRegistry, metricName, () -> metric);

    // Then the listener is notified
    onAddMethod.apply(verify(listener), metricName, metric);

    // When removing a metric in the prefix
    prefixingRegistry.remove(metricName);

    // Then the listener is modified
    onRemoveMethod.apply(verify(listener), metricName);

    // When creating or removing a metric outside of the prefix
    createMethod.apply(backingRegistry, otherMetricName, () -> metric);
    backingRegistry.remove(otherMetricName);

    // Then the listener is not notified
    verifyNoMoreInteractions(listener);

    // When removing the listener and creating or removing a metric in the prefix
    prefixingRegistry.removeListener(listener);
    createMethod.apply(prefixingRegistry, metricName, () -> metric);
    prefixingRegistry.remove(metricName);

    // Then the listener is not notified
    verifyNoMoreInteractions(listener);
  }

  @SuppressWarnings("rawtypes")
  public static Stream<Arguments> listenerMethods() {
    return Stream.of(
        arguments(
            (GetOrSupplyMethod<Counter>) MetricRegistry::counter,
            (ListenerOnAddMethod<Counter>) MetricRegistryListener::onCounterAdded,
            (ListenerOnRemoveMethod) MetricRegistryListener::onCounterRemoved,
            Counter.class),
        arguments(
            (GetOrSupplyMethod<Histogram>) MetricRegistry::histogram,
            (ListenerOnAddMethod<Histogram>) MetricRegistryListener::onHistogramAdded,
            (ListenerOnRemoveMethod) MetricRegistryListener::onHistogramRemoved,
            Histogram.class),
        arguments(
            (GetOrSupplyMethod<Meter>) MetricRegistry::meter,
            (ListenerOnAddMethod<Meter>) MetricRegistryListener::onMeterAdded,
            (ListenerOnRemoveMethod) MetricRegistryListener::onMeterRemoved,
            Meter.class),
        arguments(
            (GetOrSupplyMethod<Timer>) MetricRegistry::timer,
            (ListenerOnAddMethod<Timer>) MetricRegistryListener::onTimerAdded,
            (ListenerOnRemoveMethod) MetricRegistryListener::onTimerRemoved,
            Timer.class),
        arguments(
            (GetOrSupplyMethod<Gauge>) MetricRegistry::gauge,
            (ListenerOnAddMethod<Gauge>) MetricRegistryListener::onGaugeAdded,
            (ListenerOnRemoveMethod) MetricRegistryListener::onGaugeRemoved,
            Gauge.class));
  }

  @ParameterizedTest
  @MethodSource("getByTypeMethods")
  public <MetricT extends Metric> void should_get_all_by_type(
      GetByTypeMethod<MetricT> getByTypeMethod,
      GetByTypeWithFilterMethod<MetricT> getByTypeWithFilterMethod,
      Class<MetricT> metricClass) {

    // Given
    String name1 = "path.to.metric1";
    MetricT metric1 = mock(metricClass);
    prefixingRegistry.register(name1, metric1);
    String name2 = "path.to.metric2";
    MetricT metric2 = mock(metricClass);
    prefixingRegistry.register(name2, metric2);
    String nameInOtherPrefix = "otherPrefix.path.to.metric1";
    MetricT metricInOtherPrefix = mock(metricClass);
    backingRegistry.register(nameInOtherPrefix, metricInOtherPrefix);

    // When
    SortedMap<String, MetricT> allMetrics = getByTypeMethod.apply(prefixingRegistry);

    // Then
    assertThat(allMetrics).hasSize(2).containsEntry(name1, metric1).containsEntry(name2, metric2);

    // When
    SortedMap<String, MetricT> allName1Metrics =
        getByTypeWithFilterMethod.apply(
            prefixingRegistry, (name, metric) -> name.contains("metric1"));

    // Then
    assertThat(allName1Metrics).hasSize(1).containsEntry(name1, metric1);
  }

  @SuppressWarnings("rawtypes")
  public static Stream<Arguments> getByTypeMethods() {
    return Stream.of(
        arguments(
            (GetByTypeMethod<Counter>) MetricRegistry::getCounters,
            (GetByTypeWithFilterMethod<Counter>) MetricRegistry::getCounters,
            Counter.class),
        arguments(
            (GetByTypeMethod<Histogram>) MetricRegistry::getHistograms,
            (GetByTypeWithFilterMethod<Histogram>) MetricRegistry::getHistograms,
            Histogram.class),
        arguments(
            (GetByTypeMethod<Meter>) MetricRegistry::getMeters,
            (GetByTypeWithFilterMethod<Meter>) MetricRegistry::getMeters,
            Meter.class),
        arguments(
            (GetByTypeMethod<Timer>) MetricRegistry::getTimers,
            (GetByTypeWithFilterMethod<Timer>) MetricRegistry::getTimers,
            Timer.class),
        arguments(
            (GetByTypeMethod<Gauge>) MetricRegistry::getGauges,
            (GetByTypeWithFilterMethod<Gauge>) MetricRegistry::getGauges,
            Gauge.class));
  }

  @Test
  public void should_get_all_metrics() {
    // Given
    String counterName = "path.to.counter";
    Counter counter = prefixingRegistry.counter(counterName);
    String meterName = "path.to.meter";
    Meter meter = prefixingRegistry.meter(meterName);
    String otherPrefixCounterName = "otherPrefix.path.to.counter";
    backingRegistry.counter(otherPrefixCounterName);

    // When
    Map<String, Metric> metrics = prefixingRegistry.getMetrics();

    // Then
    assertThat(metrics)
        .hasSize(2)
        .containsEntry(counterName, counter)
        .containsEntry(meterName, meter);
  }

  private static String addPrefix(String name) {
    return PREFIX + "." + name;
  }

  interface GetOrSupplyMethod<MetricT extends Metric> {
    MetricT apply(MetricRegistry target, String name, MetricSupplier<MetricT> supplier);
  }

  interface ListenerOnAddMethod<MetricT extends Metric> {
    void apply(MetricRegistryListener target, String name, MetricT metric);
  }

  interface ListenerOnRemoveMethod {
    void apply(MetricRegistryListener target, String name);
  }

  interface GetByTypeMethod<MetricT extends Metric> {
    SortedMap<String, MetricT> apply(MetricRegistry target);
  }

  interface GetByTypeWithFilterMethod<MetricT extends Metric> {
    SortedMap<String, MetricT> apply(MetricRegistry target, MetricFilter filter);
  }
}
