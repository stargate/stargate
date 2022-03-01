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
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.config.store.api.MissingModuleSettingsException;
import io.stargate.config.store.yaml.metrics.CacheMetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigStoreYaml implements ConfigStore {
  private static final Logger logger = LoggerFactory.getLogger(ConfigStoreActivator.class);

  private static final ObjectMapper mapper = new YAMLMapper();
  private final Path configFilePath;

  final LoadingCache<Path, Map<String, Map<String, Object>>> configFileCache;
  public static final Duration DEFAULT_EVICTION_TIME = Duration.ofSeconds(30);

  public ConfigStoreYaml(Path configFilePath, MetricRegistry metricRegistry) {
    this(configFilePath, Ticker.systemTicker(), metricRegistry);
  }

  public ConfigStoreYaml(Path configFilePath, Ticker ticker, MetricRegistry metricRegistry) {
    this.configFilePath = configFilePath;
    configFileCache =
        Caffeine.newBuilder()
            .ticker(ticker)
            .maximumSize(1000)
            .expireAfterWrite(DEFAULT_EVICTION_TIME)
            .recordStats()
            .build(ConfigStoreYaml::loadConfig);
    CacheMetricsRegistry.registerCacheMetrics(metricRegistry, configFileCache);
  }

  static Map<String, Map<String, Object>> loadConfig(Path configFilePath) throws IOException {
    final File f = configFilePath.toFile();
    if (!f.exists()) {
      throw new IOException(
          String.format(
              "Can not load YAML config file '%s' (Path '%s'): does not exist", f, configFilePath));
    }
    if (!f.canRead()) {
      throw new IOException(
          String.format(
              "Can not load YAML config file '%s' (Path '%s'): not readable", f, configFilePath));
    }
    return (Map<String, Map<String, Object>>) mapper.readValue(f, Map.class);
  }

  @Override
  public ConfigWithOverrides getConfigForModule(String moduleName)
      throws MissingModuleSettingsException {
    try {
      Map<String, Map<String, Object>> result = configFileCache.get(configFilePath);
      Map<String, Object> config = result.get(moduleName);
      if (config == null) {
        throw new MissingModuleSettingsException(
            String.format(
                "The loaded configuration map (from '%s'): %s, does not contain settings from a given module: %s",
                configFilePath, result, moduleName));
      }
      logger.info(
          "Successfully loaded YAML config file (with %d entries) for module '{}' (from '{}')",
          config.size(), moduleName, configFilePath);
      return new ConfigWithOverrides(Collections.unmodifiableMap(config), moduleName);
    } catch (CompletionException e) {
      throw new CompletionException(
          String.format(
              "Problem when trying to load YAML config file for module '%s' (from: '%s'): %s",
              moduleName, configFilePath, e.getMessage()),
          e);
    }
  }
}
