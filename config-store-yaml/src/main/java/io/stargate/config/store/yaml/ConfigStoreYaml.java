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
package io.stargate.config.store.yaml;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.config.store.api.MissingModuleSettingsException;
import io.stargate.config.store.yaml.metrics.CacheMetricsRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

public class ConfigStoreYaml implements ConfigStore {
  private final ObjectMapper mapper;
  private final Path configFilePath;
  private final MapType yamlConfigType;
  @VisibleForTesting final LoadingCache<Path, Map<String, Map<String, Object>>> configFileCache;
  public static final Duration DEFAULT_EVICTION_TIME = Duration.ofSeconds(30);

  public ConfigStoreYaml(Path configFilePath, MetricRegistry metricRegistry) {
    this(configFilePath, Ticker.systemTicker(), metricRegistry);
  }

  @VisibleForTesting
  public ConfigStoreYaml(Path configFilePath, Ticker ticker, MetricRegistry metricRegistry) {
    this.configFilePath = configFilePath;
    mapper = new ObjectMapper(new YAMLFactory());
    MapType mapType =
        mapper.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class);
    yamlConfigType =
        mapper
            .getTypeFactory()
            .constructMapType(
                HashMap.class, mapper.getTypeFactory().constructType(String.class), mapType);

    configFileCache =
        CacheBuilder.newBuilder()
            .ticker(ticker)
            .expireAfterWrite(DEFAULT_EVICTION_TIME)
            .recordStats()
            .build(
                new CacheLoader<Path, Map<String, Map<String, Object>>>() {
                  @Override
                  public Map<String, Map<String, Object>> load(@Nonnull Path configFilePath)
                      throws Exception {
                    return mapper.readValue(configFilePath.toFile(), yamlConfigType);
                  }
                });

    CacheMetricsRegistry.registerCacheMetrics(metricRegistry, configFileCache);
  }

  @Override
  public ConfigWithOverrides getConfigForModule(String moduleName)
      throws MissingModuleSettingsException {
    try {
      Map<String, Map<String, Object>> result = configFileCache.get(configFilePath);
      if (!result.containsKey(moduleName)) {
        throw new MissingModuleSettingsException(
            String.format(
                "The loaded configuration map: %s, does not contain settings from a given module: %s",
                result, moduleName));
      }
      return new ConfigWithOverrides(ImmutableMap.copyOf(result.get(moduleName)), moduleName);
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(
          "Problem when processing yaml file from: " + configFilePath, e);
    }
  }
}
