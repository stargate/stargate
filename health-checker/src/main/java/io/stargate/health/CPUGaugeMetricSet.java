package io.stargate.health;

import static oshi.hardware.CentralProcessor.TickType.IDLE;
import static oshi.hardware.CentralProcessor.TickType.IOWAIT;
import static oshi.hardware.CentralProcessor.TickType.NICE;
import static oshi.hardware.CentralProcessor.TickType.SYSTEM;
import static oshi.hardware.CentralProcessor.TickType.USER;

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
  private final int TIME_BETWEEN_TICKS = 950;

  public CPUGaugeMetricSet() {
    this.processor = new SystemInfo().getHardware().getProcessor();
    this.prevTicks = new long[TickType.values().length];
    this.curTicks = new long[TickType.values().length];
  }

  protected CPUGaugeMetricSet(CentralProcessor processor, long[] curTicks, long[] prevTicks) {
    this.processor = processor;
    this.prevTicks = prevTicks;
    this.curTicks = curTicks;
  }

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<>();

    gauges.put("load", (Gauge<Double>) this::getSystemCpuLoad);
    gauges.put("user", (Gauge<Double>) () -> getTick(USER));
    gauges.put("nice", (Gauge<Double>) () -> getTick(NICE));
    gauges.put("sys", (Gauge<Double>) () -> getTick(SYSTEM));
    gauges.put("idle", (Gauge<Double>) () -> getTick(IDLE));
    gauges.put("iowait", (Gauge<Double>) () -> getTick(IOWAIT));

    return Collections.unmodifiableMap(gauges);
  }

  private Double getSystemCpuLoad() {
    long now = System.currentTimeMillis();
    if (now - this.tickTime > TIME_BETWEEN_TICKS) {
      // Enough time has elapsed.
      updateSystemTicks();
    }

    return processor.getSystemCpuLoadBetweenTicks(prevTicks);
  }

  private Double getTick(TickType tickType) {
    long now = System.currentTimeMillis();
    if (now - this.tickTime > TIME_BETWEEN_TICKS) {
      // Enough time has elapsed.
      updateSystemTicks();
    }

    long user = curTicks[USER.getIndex()] - prevTicks[USER.getIndex()];
    long nice = curTicks[NICE.getIndex()] - prevTicks[NICE.getIndex()];
    long sys = curTicks[SYSTEM.getIndex()] - prevTicks[SYSTEM.getIndex()];
    long idle = curTicks[IDLE.getIndex()] - prevTicks[IDLE.getIndex()];
    long iowait = curTicks[IOWAIT.getIndex()] - prevTicks[IOWAIT.getIndex()];
    long irq = curTicks[TickType.IRQ.getIndex()] - prevTicks[TickType.IRQ.getIndex()];
    long softirq = curTicks[TickType.SOFTIRQ.getIndex()] - prevTicks[TickType.SOFTIRQ.getIndex()];
    long steal = curTicks[TickType.STEAL.getIndex()] - prevTicks[TickType.STEAL.getIndex()];
    long totalCPU = user + nice + sys + idle + iowait + irq + softirq + steal;

    switch (tickType) {
      case USER:
        return (double) user / (double) totalCPU;
      case NICE:
        return (double) nice / (double) totalCPU;
      case SYSTEM:
        return (double) sys / (double) totalCPU;
      case IDLE:
        return (double) idle / (double) totalCPU;
      case IOWAIT:
        return (double) iowait / (double) totalCPU;
      default:
        return 0.0;
    }
  }

  private void updateSystemTicks() {
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
