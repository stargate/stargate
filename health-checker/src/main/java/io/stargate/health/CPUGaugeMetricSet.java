package io.stargate.health;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;

public class CPUGaugeMetricSet implements MetricSet {

  final CentralProcessor processor;
  private long tickTime;
  private final long[] prevTicks;
  private final long[] curTicks;

  public CPUGaugeMetricSet() {
    processor = new SystemInfo().getHardware().getProcessor();
    this.prevTicks = new long[TickType.values().length];
    this.curTicks = new long[TickType.values().length];
  }

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<>();

    gauges.put("load", (Gauge<Double>) this::getSystemCpuLoad);
    gauges.put("user", (Gauge<Double>) () -> getTick("user"));
    gauges.put("nice", (Gauge<Double>) () -> getTick("nice"));
    gauges.put("sys", (Gauge<Double>) () -> getTick("sys"));
    gauges.put("idle", (Gauge<Double>) () -> getTick("idle"));
    gauges.put("iowait", (Gauge<Double>) () -> getTick("iowait"));

    return Collections.unmodifiableMap(gauges);
  }

  private Double getSystemCpuLoad() {
    long now = System.currentTimeMillis();
    if (now - this.tickTime > 950) {
      // Enough time has elapsed.
      updateSystemTicks();
    }

    return processor.getSystemCpuLoadBetweenTicks(prevTicks);
  }

  private Double getTick(String tickType) {
    long now = System.currentTimeMillis();
    if (now - this.tickTime > 950) {
      // Enough time has elapsed.
      updateSystemTicks();
    }

    long user = curTicks[TickType.USER.getIndex()] - prevTicks[TickType.USER.getIndex()];
    long nice = curTicks[TickType.NICE.getIndex()] - prevTicks[TickType.NICE.getIndex()];
    long sys = curTicks[TickType.SYSTEM.getIndex()] - prevTicks[TickType.SYSTEM.getIndex()];
    long idle = curTicks[TickType.IDLE.getIndex()] - prevTicks[TickType.IDLE.getIndex()];
    long iowait = curTicks[TickType.IOWAIT.getIndex()] - prevTicks[TickType.IOWAIT.getIndex()];
    long totalCPU = user + nice + sys + idle + iowait;

    switch (tickType) {
      case "user":
        return (double) user / (double) totalCPU;
      case "nice":
        return (double) nice / (double) totalCPU;
      case "sys":
        return (double) sys / (double) totalCPU;
      case "idle":
        return (double) idle / (double) totalCPU;
      case "iowait":
        return (double) iowait / (double) totalCPU;
      default:
        return 0.0;
    }
  }

  protected void updateSystemTicks() {
    long[] ticks = processor.getSystemCpuLoadTicks();
    // Skip update if ticks is all zero.
    // Iterate to find a nonzero tick value and return; this should quickly
    // find a nonzero value if one exists and be fast in checking 0's
    // through branch prediction if it doesn't
    for (long tick : ticks) {
      if (tick != 0) {
        // We have a nonzero tick array, update and return!
        this.tickTime = System.currentTimeMillis();
        // Copy to previous
        System.arraycopy(this.curTicks, 0, this.prevTicks, 0, this.curTicks.length);
        System.arraycopy(ticks, 0, this.curTicks, 0, ticks.length);
        return;
      }
    }
  }
}
