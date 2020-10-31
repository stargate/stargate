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
package io.stargate.config.store.yaml.metrics;

import static io.stargate.config.store.yaml.metrics.MetricsHelper.getMetricValue;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import io.stargate.config.store.yaml.FakeTicker;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Test;

class CacheMetricsRegistryTest {

  @Test
  public void shouldRegisterCacheMetrics() throws ExecutionException {
    // given
    Duration evictionTime = Duration.ofSeconds(1);
    FakeTicker fakeTicker = new FakeTicker();
    LoadingCache<String, String> cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(evictionTime)
            .ticker(fakeTicker)
            .recordStats()
            .build(
                new CacheLoader<String, String>() {
                  @Override
                  public String load(@Nonnull String key) {
                    return key.toUpperCase();
                  }
                });
    MetricRegistry metricRegistry = new MetricRegistry();
    CacheMetricsRegistry.registerCacheMetrics(metricRegistry, cache);

    // when
    cache.get("a");
    cache.get("b");
    fakeTicker.advance(evictionTime);
    cache.get("a");
    cache.get("a");

    // then
    CacheStats stats = cache.stats();
    assertThat(stats.hitCount())
        .isEqualTo(getMetricValue(metricRegistry, CacheMetricsRegistry.HIT_COUNT))
        .isEqualTo(1);

    assertThat(stats.evictionCount())
        .isEqualTo(getMetricValue(metricRegistry, CacheMetricsRegistry.EVICTION_COUNT))
        .isEqualTo(1);

    assertThat(stats.hitRate())
        .isEqualTo(getMetricValue(metricRegistry, CacheMetricsRegistry.HIT_RATE))
        .isEqualTo(0.25);

    assertThat(stats.missCount())
        .isEqualTo(getMetricValue(metricRegistry, CacheMetricsRegistry.MISS_COUNT))
        .isEqualTo(3);

    assertThat(stats.missRate())
        .isEqualTo(getMetricValue(metricRegistry, CacheMetricsRegistry.MISS_RATE))
        .isEqualTo(0.75);

    assertThat(cache.size())
        .isEqualTo(getMetricValue(metricRegistry, CacheMetricsRegistry.SIZE))
        .isEqualTo(2);
  }
}
