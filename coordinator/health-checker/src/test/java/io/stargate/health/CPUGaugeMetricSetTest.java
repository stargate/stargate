package io.stargate.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import java.util.Map;
import org.junit.jupiter.api.Test;
import oshi.hardware.CentralProcessor;

class CPUGaugeMetricSetTest {

  @Test
  @SuppressWarnings("unchecked")
  void getMetrics() {
    CPUGaugeMetricSet cpuGaugeMetricSet = new CPUGaugeMetricSet();
    Map<String, Metric> metrics = cpuGaugeMetricSet.getMetrics();

    assertThat(((Gauge<Double>) metrics.get("load")).getValue()).isBetween(0.0, 1.0);
    assertThat(((Gauge<Double>) metrics.get("user")).getValue()).isBetween(0.0, 1.0);
    assertThat(((Gauge<Double>) metrics.get("nice")).getValue()).isBetween(0.0, 1.0);
    assertThat(((Gauge<Double>) metrics.get("sys")).getValue()).isBetween(0.0, 1.0);
    assertThat(((Gauge<Double>) metrics.get("idle")).getValue()).isBetween(0.0, 1.0);
    assertThat(((Gauge<Double>) metrics.get("iowait")).getValue()).isBetween(0.0, 1.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  void getMetricsWithMock() {
    long[] curTicks = new long[] {9, 10, 11, 12, 13, 14, 15, 16};
    long[] prevTicks = new long[] {0, 0, 0, 0, 0, 0, 0, 0};

    CentralProcessor processor = mock(CentralProcessor.class);
    CPUGaugeMetricSet cpuGaugeMetricSet = new CPUGaugeMetricSet(processor, curTicks, prevTicks);

    when(processor.getSystemCpuLoadTicks()).thenReturn(new long[0]);
    when(processor.getSystemCpuLoadBetweenTicks(any())).thenReturn(0.1);

    Map<String, Metric> metrics = cpuGaugeMetricSet.getMetrics();

    assertThat(((Gauge<Double>) metrics.get("load")).getValue()).isEqualTo(0.1);
    assertThat(((Gauge<Double>) metrics.get("user")).getValue()).isEqualTo(0.09);
    assertThat(((Gauge<Double>) metrics.get("nice")).getValue()).isEqualTo(0.1);
    assertThat(((Gauge<Double>) metrics.get("sys")).getValue()).isEqualTo(0.11);
    assertThat(((Gauge<Double>) metrics.get("idle")).getValue()).isEqualTo(0.12);
    assertThat(((Gauge<Double>) metrics.get("iowait")).getValue()).isEqualTo(0.13);
  }
}
