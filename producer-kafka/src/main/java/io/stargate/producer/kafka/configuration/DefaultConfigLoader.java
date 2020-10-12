/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka.configuration;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.dropwizard.kafka.metrics.DropwizardMetricsReporter;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;

public class DefaultConfigLoader implements ConfigLoader {

  private static final Pattern CDC_KAFKA_PRODUCER_SETTING_PATTERN =
      Pattern.compile(String.format("^(%s)\\.(.*.)", CDC_KAFKA_PRODUCER_SETTING_PREFIX));

  @Override
  public CDCKafkaConfig loadConfig(Map<String, Object> options) {
    String topicPrefixName = getTopicPrefixName(options);
    Map<String, Object> kafkaProducerSettings = filterKafkaProducerSettings(options);
    MetricsConfig metricsConfig = loadMetricsConfig(options);
    registerMetricsIfEnabled(kafkaProducerSettings, metricsConfig);
    String schemaRegistryUrl = getSchemaRegistryUrl(kafkaProducerSettings);
    return new CDCKafkaConfig(
        topicPrefixName, schemaRegistryUrl, kafkaProducerSettings, metricsConfig);
  }

  private void registerMetricsIfEnabled(
      Map<String, Object> kafkaProducerSettings, MetricsConfig metricsConfig) {
    if (metricsConfig.isMetricsEnabled()) {
      kafkaProducerSettings.put(
          CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
          DropwizardMetricsReporter.class.getName());
      // create dropwizard specific settings
      kafkaProducerSettings.put(
          DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
          Boolean.toString(metricsConfig.isIncludeTags()));
      kafkaProducerSettings.put(
          DropwizardMetricsReporter.METRICS_NAME_CONFIG, metricsConfig.getMetricsName());
    }
  }

  private MetricsConfig loadMetricsConfig(Map<String, Object> options) {
    boolean metricsEnabled = getBooleanSettingValue(options, METRICS_ENABLED_SETTING_NAME);
    if (metricsEnabled) {
      // load specific settings only if metrics are enabled
      boolean metricsIncludeTags =
          getBooleanSettingValue(options, METRICS_INCLUDE_TAGS_SETTING_NAME);
      String metricsName = getStringSettingValue(options, METRICS_NAME_SETTING_NAME);
      return new MetricsConfig(metricsEnabled, metricsIncludeTags, metricsName);
    } else {
      return new MetricsConfig(metricsEnabled, null, null);
    }
  }

  Map<String, Object> filterKafkaProducerSettings(Map<String, Object> options) {
    return options.entrySet().stream()
        .filter(this::isKafkaProducerSetting)
        .map(this::toKafkaProducerSetting)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @NonNull
  Entry<String, Object> toKafkaProducerSetting(Entry<String, Object> entry) {
    Matcher matcher = CDC_KAFKA_PRODUCER_SETTING_PATTERN.matcher(entry.getKey());
    if (matcher.matches()) {
      return new SimpleEntry<>(matcher.group(2), entry.getValue());
    } else {
      throw new IllegalStateException(
          String.format(
              "Matcher for pattern: %s and entry.key: %s should match",
              CDC_KAFKA_PRODUCER_SETTING_PATTERN, entry.getKey()));
    }
  }

  private boolean isKafkaProducerSetting(Entry<String, Object> entry) {
    return CDC_KAFKA_PRODUCER_SETTING_PATTERN.matcher(entry.getKey()).matches();
  }

  String getTopicPrefixName(Map<String, Object> options) {
    return getStringSettingValue(options, CDC_TOPIC_PREFIX_NAME);
  }

  String getSchemaRegistryUrl(Map<String, Object> kafkaProducerSettings) {
    return getStringSettingValue(kafkaProducerSettings, SCHEMA_REGISTRY_URL_SETTING_NAME);
  }

  @NonNull
  String getStringSettingValue(Map<String, Object> options, String settingName) {
    return (String) getSettingValue(options, settingName, String.class);
  }

  @NonNull
  Boolean getBooleanSettingValue(Map<String, Object> options, String settingName) {
    return (Boolean) getSettingValue(options, settingName, Boolean.class);
  }

  @NonNull
  Object getSettingValue(Map<String, Object> options, String settingName, Class<?> expectedType) {
    Object prefixName = options.get(settingName);
    if (prefixName == null) {
      throw new IllegalArgumentException(
          String.format("The config value for %s is not present", settingName));
    }
    if (!(prefixName.getClass().isAssignableFrom(expectedType))) {
      throw new IllegalArgumentException(
          String.format(
              "The config value for %s has wrong type: %s. It should be of a %s type",
              settingName, prefixName.getClass().getName(), expectedType.getName()));
    }
    return prefixName;
  }
}
