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

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;

public class CacheMetricsRegistry {
  public static final String CACHE_NAME = "file-cache";

  public static final String HIT_COUNT = "hitCount";

  public static final String HIT_RATE = "hitRate";

  public static final String MISS_COUNT = "missCount";

  public static final String MISS_RATE = "missRate";

  public static final String EVICTION_COUNT = "evictionCount";

  public static final String SIZE = "size";

  /**
   * Registers all cache metrics in the given metric registry. It uses {@link
   * CacheMetricsRegistry#CACHE_NAME} as a metrics prefix.
   */
  public static void registerCacheMetrics(MetricRegistry metricRegistry, Cache<?, ?> cache) {

    metricRegistry.register(
        nameWithPrefix(HIT_RATE), (Gauge<Double>) () -> cache.stats().hitRate());

    metricRegistry.register(
        nameWithPrefix(HIT_COUNT), (Gauge<Long>) () -> cache.stats().hitCount());

    metricRegistry.register(
        nameWithPrefix(MISS_COUNT), (Gauge<Long>) () -> cache.stats().missCount());

    metricRegistry.register(
        nameWithPrefix(MISS_RATE), (Gauge<Double>) () -> cache.stats().missRate());

    metricRegistry.register(
        nameWithPrefix(EVICTION_COUNT), (Gauge<Long>) () -> cache.stats().evictionCount());

    metricRegistry.register(nameWithPrefix(SIZE), (Gauge<Long>) cache::estimatedSize);
  }

  private static String nameWithPrefix(String hitRate) {
    return name(CACHE_NAME, hitRate);
  }
}
