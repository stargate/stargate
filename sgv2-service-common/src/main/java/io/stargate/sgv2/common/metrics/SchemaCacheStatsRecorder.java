package io.stargate.sgv2.common.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import java.util.concurrent.TimeUnit;

/**
 * Implementation inspired by <a
 * href="https://metrics.dropwizard.io/4.2.0/manual/caffeine.html">Dropwizart-metrics for
 * Caffeine</a> but modified for out needs.
 */
public class SchemaCacheStatsRecorder implements StatsCounter {
  private final Meter hits, misses;

  private final Timer loadTimes;

  private SchemaCacheStatsRecorder(MetricRegistry metricRegistry, String prefix) {
    hits = metricRegistry.meter(prefix + ".hits");
    misses = metricRegistry.meter(prefix + ".misses");
    loadTimes = metricRegistry.timer(prefix + ".loadtimes");
    // Let's calculate just one timed hit ratio, for past 5 minutes, calculated
    // dynamically as-needed, exposed as simple Gauge.
    // Would be easy to add others (including total lifetime) if that seemed useful.
    final Gauge<Double> hitRatio5 =
        new Gauge<Double>() {
          @Override
          public Double getValue() {
            double hitRate = hits.getFiveMinuteRate();
            double totalRate = hitRate + misses.getFiveMinuteRate();
            // To avoid division-by-zero, just claim 100% hit rate if not much data
            if (totalRate < 0.0000001) {
              return 100.0;
            }
            return hitRate / totalRate;
          }
        };
    metricRegistry.register(prefix + ".hitRatePct5Minutes", hitRatio5);
  }

  public static SchemaCacheStatsRecorder create(MetricRegistry metricRegistry, String prefix) {
    return new SchemaCacheStatsRecorder(metricRegistry, prefix);
  }

  @Override
  public void recordHits(int count) {
    hits.mark(count);
  }

  @Override
  public void recordMisses(int count) {
    misses.mark(count);
  }

  @Override
  public void recordLoadSuccess(long loadTime) {
    System.err.println("CACHE-LOAD-SUCCESS: " + loadTime);
    loadTimes.update(loadTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordLoadFailure(long loadTime) {
    System.err.println("CACHE-LOAD-FAIL: " + loadTime);
    loadTimes.update(loadTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordEviction() {}

  @Override
  public CacheStats snapshot() {
    // We don't actually care about most of these things; could easily be implemented
    // if we did
    return CacheStats.of(
        hits.getCount(),
        misses.getCount(),
        0L,
        0L, // loadSuccess, loadFailure
        0L, // totalLoadTime
        0L, // evictionCount
        0L // evictionWeight
        );
  }
}
